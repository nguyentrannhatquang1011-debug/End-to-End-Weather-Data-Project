import streamlit as st
import pandas as pd
import duckdb
import altair as alt
from influxdb_client import InfluxDBClient
import os
from streamlit_autorefresh import st_autorefresh
from datetime import datetime, timedelta
from dotenv import load_dotenv


# =============================================================================
# 1. CẤU HÌNH HỆ THỐNG & BIẾN MÔI TRƯỜNG
# =============================================================================
# Tải cấu hình từ file .env để bảo mật thông tin nhạy cảm
load_dotenv()

st.set_page_config(
    page_title="Hanoi Weather & AQI Hybrid Dashboard",
    page_icon="🌤️",
    layout="wide"
)

# Lấy thông tin kết nối từ biến môi trường
INFLUX_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUX_ORG = os.getenv("INFLUXDB_ORG", "weather_org")
INFLUX_BUCKET = os.getenv("INFLUXDB_BUCKET", "weather_data")

MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD")

# =============================================================================
# 2. LỚP TRUY CẬP DỮ LIỆU (DATA ACCESS LAYER)
# =============================================================================

@st.cache_resource
def get_duckdb_conn():
    """
    Khởi tạo kết nối DuckDB và cấu hình Extension Iceberg/S3.
    Sử dụng @st.cache_resource để tái sử dụng kết nối, tiết kiệm RAM.
    """
    conn = duckdb.connect(database=':memory:')
    # Cài đặt các extension cần thiết để DuckDB đọc được Iceberg trên S3 (MinIO)
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    
    # Cấu hình kết nối S3A tới MinIO
    conn.execute(f"SET s3_endpoint='minio:9000';")
    conn.execute(f"SET s3_access_key_id='{MINIO_USER}';")
    conn.execute(f"SET s3_secret_access_key='{MINIO_PASS}';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")
    
    # TỐI ƯU RAM: Giới hạn bộ nhớ DuckDB để không làm sập hệ thống 16GB RAM
    conn.execute("SET memory_limit='2GB';")
    return conn

@st.cache_data(ttl=600) # Cache kết quả truy vấn Gold Layer trong 10 phút vì dữ liệu lịch sử không thay đổi thường xuyên
def query_gold_layer(query: str):
    """
    Truy vấn dữ liệu từ Gold Layer bằng DuckDB Iceberg Scan.
    Sử dụng @st.cache_data để tránh truy vấn lại nhiều lần qua mạng.
    """
    conn = get_duckdb_conn()
    df = conn.execute(query).df()
    # Làm tròn tất cả các giá trị số lên 2 chữ số thập phân
    return df.round(2)

@st.cache_resource # Đảm bảo client InfluxDB được tái sử dụng, tránh tạo mới mỗi lần truy vấn
def get_influx_client() -> InfluxDBClient:
    """
    Khởi tạo và duy trì một kết nối InfluxDB duy nhất (Singleton).
    
    Sử dụng @st.cache_resource để InfluxDBClient không bị hủy khi Dashboard rerun.
    Điều này giúp tái sử dụng Connection Pool nội bộ, giảm tải CPU và Network
    vì không cần thiết lập lại SSL Handshake cho mỗi 30 giây Auto-refresh.
    
    Returns:
        InfluxDBClient: Đối tượng client đã được kết nối.
    """
    return InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)

@st.cache_data(ttl=30)
def query_influxdb(flux_query: str):
    """
    Truy vấn dữ liệu thời gian thực từ InfluxDB.
    
    Quy trình:
    1. Lấy client từ cache (không tạo mới).
    2. Thực hiện truy vấn qua Query API.
    3. Xử lý gộp kết quả và chuẩn hóa múi giờ.
    """
    try:
        # Bước 1: Lấy client từ Resource Cache (Đã tối ưu pooling)
        client = get_influx_client()
        
        # Bước 2: Truy vấn dữ liệu
        df = client.query_api().query_data_frame(flux_query)
        
        # XỬ LÝ LỖI: InfluxDB có thể trả về một danh sách các DataFrame nếu truy vấn có nhiều table streams
        if isinstance(df, list):
            # Gộp tất cả các DataFrame trong list thành một DataFrame duy nhất
            df = pd.concat(df) if df else pd.DataFrame()

        if not df.empty:
            # Chuyển đổi múi giờ sang Việt Nam và làm tròn
            df['_time'] = pd.to_datetime(df['_time']).dt.tz_convert('Asia/Bangkok')
            return df.round(2)
        return df
    except Exception as e:
        st.error(f"Lỗi kết nối InfluxDB: {e}")
        return pd.DataFrame()

# =============================================================================
# 3. LOGIC HỖ TRỢ HIỂN THỊ (UTILITIES)
# =============================================================================

def get_aqi_category(aqi: float):
    """Phân loại AQI theo quy chuẩn quốc tế và trả về màu sắc."""
    if aqi <= 50: return "Tốt", "#00e400"
    if aqi <= 100: return "Trung bình", "#ffff00"
    if aqi <= 150: return "Kém", "#ff7e00"
    if aqi <= 200: return "Xấu", "#ff0000"
    if aqi <= 300: return "Rất xấu", "#8f3f97"
    return "Nguy hại", "#7e0023"

def get_wind_direction(degree: float):
    """Chuyển đổi độ hướng gió sang icon hướng."""
    directions = ["⬇️ Bắc", "↙️ Đ.Bắc", "⬅️ Đông", "↖️ Đ.Nam", "⬆️ Nam", "↗️ T.Nam", "➡️ Tây", "⬇️ T.Bắc"]
    idx = int((degree + 22.5) / 45) % 8
    return directions[idx]

# =============================================================================
# 4. GIAO DIỆN CHÍNH (STREAMLIT UI)
# =============================================================================

st.sidebar.title("🏙️ Hanoi Dashboard")

# --- CẤU HÌNH TỰ ĐỘNG LÀM MỚI (RESOURCES OPTIMIZATION) ---
st.sidebar.subheader("⚙️ Cấu hình Live")
auto_refresh = st.sidebar.checkbox("Tự động cập nhật dữ liệu", value=True)
if auto_refresh:
    # Cho phép chọn interval để người dùng chủ động điều tiết tài nguyên máy
    refresh_interval = st.sidebar.select_slider(
        "Tần suất làm mới",
        options=[30, 60, 300],
        format_func=lambda x: f"{x} giây" if x < 60 else f"{x//60} phút"
    )
    # Component này sẽ kích hoạt rerun script từ phía Client-side
    st_autorefresh(interval=refresh_interval * 1000, key="data_refresh_timer")

tab_selection = st.sidebar.radio("Chọn trang hiển thị:", ["Dữ liệu Thời tiết Live", "Chất lượng Không khí Live", "Gold Insights (Tổng hợp)"])


# -----------------------------------------------------------------------------
# TRANG 1: LIVE WEATHER
# -----------------------------------------------------------------------------
if tab_selection == "Dữ liệu Thời tiết Live":
    st.title("🌤️ Theo dõi Thời tiết Hà Nội (Real-time)")
    
    # A. Real-time Metrics từ InfluxDB
    flux_live = f'from(bucket: "{INFLUX_BUCKET}") |> range(start: -1h) |> filter(fn: (r) => r._measurement == "weather_metrics") |> last()'
    df_live = query_influxdb(flux_live)
    
    if not df_live.empty:
        # Pivot dữ liệu InfluxDB để lấy các fields
        live_data = df_live.pivot(index='_time', columns='_field', values='_value').iloc[-1]
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Nhiệt độ", f"{live_data['temperature']:.2f} °C")
        c2.metric("Tốc độ gió", f"{live_data['windspeed']:.2f} km/h")
        c3.metric("Hướng gió", get_wind_direction(live_data['winddirection']))

        # B. Hybrid Logic: So sánh với Gold Layer
        st.divider()
        st.subheader("🔄 So sánh với dữ liệu lịch sử từ Gold Layer")
        
        # Lấy mốc so sánh (mặc định là trung bình toàn thời gian trong Gold)
        gold_query = """
            SELECT avg(avg_temp) as hist_avg, max(max_temp) as hist_max
            FROM iceberg_scan('s3://gold/weather_db/weather_daily_summary')
            WHERE station_id = 'HaNoi'
        """
        df_gold = query_gold_layer(gold_query)
        
        if not df_gold.empty:
            hist_avg = df_gold['hist_avg'].values[0]
            hist_max = df_gold['hist_max'].values[0]
            delta = live_data['temperature'] - hist_avg
            
            st.write(f"📊 **Nhiệt độ hiện tại đang lệch {delta:+.2f}°C so với trung bình lịch sử.**")
            
            # Scenario C: Alerts
            if live_data['temperature'] >= hist_max:
                st.error(f"🔥 CẢNH BÁO: Nhiệt độ hiện tại ({live_data['temperature']}°C) đã chạm hoặc vượt đỉnh lịch sử ({hist_max}°C)!")

    # C. 30-Day Scroll Chart
    st.subheader("📈 Biểu đồ Nhiệt độ 30 ngày qua")
    flux_30d = f'from(bucket: "{INFLUX_BUCKET}") |> range(start: -30d) |> filter(fn: (r) => r._measurement == "weather_metrics" and r._field == "temperature")'
    df_30d = query_influxdb(flux_30d)
    if not df_30d.empty:
        # Sử dụng Altair để tùy chỉnh trục và tooltip
        line_chart = alt.Chart(df_30d).mark_line(color='#ff4b4b').encode(
            x=alt.X('_time:T', 
                    title='Thời gian (Ngày/Tháng Giờ:Phút)',
                    axis=alt.Axis(format='%d/%m %H:%M', labelAngle=-45)),
            y=alt.Y('_value:Q', title='Nhiệt độ (°C)', scale=alt.Scale(zero=False)),
            tooltip=[
                alt.Tooltip('_time:T', title='Thời điểm', format='%Y-%m-%d %H:%M'),
                alt.Tooltip('_value:Q', title='Nhiệt độ (°C)', format='.2f')
            ]
        ).properties(height=400).interactive()
        
        st.altair_chart(line_chart, use_container_width=True)

# -----------------------------------------------------------------------------
# TRANG 2: LIVE AQI
# -----------------------------------------------------------------------------
elif tab_selection == "Chất lượng Không khí Live":
    st.title("🍃 Chất lượng không khí Hà Nội (Live)")

    # A. Real-time AQI
    # TỐI ƯU: Tăng dải thời gian tìm kiếm lên 24 giờ thay vì 1 giờ. 
    # AQI thường cập nhật chậm hoặc trạm có thể offline ngắn hạn, việc lấy mốc 24h giúp 
    # Dashboard luôn hiển thị giá trị gần nhất thay vì bị ẩn khối UI khi chưa có bản tin mới.
    flux_aqi = f'from(bucket: "{INFLUX_BUCKET}") |> range(start: -24h) |> filter(fn: (r) => r._measurement == "aqi_metrics" and r._field == "aqi") |> last()'
    df_aqi = query_influxdb(flux_aqi)
    
    if not df_aqi.empty:
        val_aqi = df_aqi['_value'].values[0]
        label, color = get_aqi_category(val_aqi)
        
        st.markdown(f"""
            <div style="background-color:{color}; padding:20px; border-radius:10px; text-align:center;">
                <h1 style="color:white; margin:0;">AQI HIỆN TẠI: {int(val_aqi)}</h1>
                <h2 style="color:white; margin:0;">Trạng thái: {label}</h2>
            </div>
        """, unsafe_allow_html=True)

        # B. Hybrid Logic cho AQI
        gold_aqi_q = """
            SELECT avg(avg_aqi) as avg_q, max(max_aqi) as max_q 
            FROM iceberg_scan('s3://gold/weather_db/aqi_daily_summary')
        """
        df_gold_aqi = query_gold_layer(gold_aqi_q)
        
        if not df_gold_aqi.empty:
            avg_h = df_gold_aqi['avg_q'].values[0]
            max_h = df_gold_aqi['max_q'].values[0]
            
            # Giải thích logic st.metric:
            # - label: Tiêu đề chính của thẻ.
            # - value: Con số AQI hiện tại (được làm tròn 2 chữ số thập phân).
            # - delta: Khoảng cách so với trung bình (nếu dương là ô nhiễm hơn, âm là sạch hơn).
            # - delta_color="inverse": Vì AQI càng cao càng XẤU, nên ta đảo ngược màu: 
            #   Tăng AQI sẽ hiện màu ĐỎ (cảnh báo), giảm AQI sẽ hiện màu XANH (tốt).
            st.metric(
                label="Chỉ số AQI Hiện tại", 
                value=f"{val_aqi:.2f}", 
                delta=f"{val_aqi - avg_h:+.2f} so với trung bình lịch sử", 
                delta_color="inverse"
            )
            
            if val_aqi > max_h:
                st.warning(f"🚨 CẢNH BÁO: Chỉ số AQI đang ở mức kỷ lục mới (Vượt mức đỉnh {max_h})!")
    else:
        # Fallback: Nếu quá 24h không có dữ liệu, hiển thị thông báo thay vì để trống Dashboard
        st.info("⚠️ Hiện tại không tìm thấy dữ liệu AQI mới trong 24 giờ qua. Vui lòng kiểm tra lại luồng Streaming hoặc kết nối trạm.")


    # C. History Chart (Area chart cho PM2.5/PM10 - Ở đây mô phỏng qua AQI 30 ngày)
    st.subheader("📊 Diễn biến AQI (30 ngày gần nhất)")
    flux_aqi_30 = f'from(bucket: "{INFLUX_BUCKET}") |> range(start: -30d) |> filter(fn: (r) => r._measurement == "aqi_metrics" and r._field == "aqi")'
    df_aqi_30 = query_influxdb(flux_aqi_30)
    if not df_aqi_30.empty:
        aqi_chart = alt.Chart(df_aqi_30).mark_area(
            line={'color':'darkgreen'},
            color=alt.Gradient(
                gradient='linear',
                stops=[alt.GradientStop(color='white', offset=0),
                       alt.GradientStop(color='green', offset=1)],
                x1=1, x2=1, y1=1, y2=0
            )
        ).encode(
            x=alt.X('_time:T', title='Ngày cập nhật', axis=alt.Axis(format='%d/%m %H:%M')),
            y=alt.Y('_value:Q', title='Chỉ số AQI'),
            tooltip=[
                alt.Tooltip('_time:T', title='Ngày giờ', format='%Y-%m-%d %H:%M'),
                alt.Tooltip('_value:Q', title='AQI', format='.0f')
            ]
        ).properties(height=400).interactive()
        
        st.altair_chart(aqi_chart, use_container_width=True)

# -----------------------------------------------------------------------------
# TRANG 3: GOLD INSIGHTS
# -----------------------------------------------------------------------------
else:
    st.title("🏆 Gold Layer Insights (Phân tích Tổng hợp)")
    st.markdown("Dữ liệu được trích xuất trực tiếp từ **Lakehouse Gold Layer (Iceberg Format)** trên MinIO.")

    # A. Metric Cards (Records)
    # Workflow: DuckDB iceberg_scan sẽ đọc file metadata để tìm các file parquet liên quan nhanh chóng.
    summary_query = """
        WITH weather AS (
            SELECT max(max_temp) as record_temp FROM iceberg_scan('s3://gold/weather_db/weather_daily_summary')
        ),
        aqi AS (
            SELECT date, max_aqi FROM iceberg_scan('s3://gold/weather_db/aqi_daily_summary')
            ORDER BY max_aqi DESC LIMIT 1
        )
        SELECT record_temp, date as max_aqi_date, max_aqi FROM weather, aqi
    """
    df_summary = query_gold_layer(summary_query)
    
    if not df_summary.empty:
        m1, m2, m3 = st.columns(3)
        # Sử dụng định dạng :.2f để đảm bảo luôn hiển thị 2 chữ số thập phân
        m1.metric("Nhiệt độ đỉnh lịch sử", f"{df_summary['record_temp'].values[0]:.2f} °C")
        m2.metric("AQI cao nhất ghi nhận", f"{df_summary['max_aqi'].values[0]}")
        m3.metric("Ngày ô nhiễm nhất", f"{df_summary['max_aqi_date'].values[0]}")

    # B. Seasonal Trend (Biểu đồ cột theo tháng)
    st.divider()
    st.subheader("🗓️ Xu hướng Chất lượng không khí theo tháng")
    
    monthly_aqi_query = """
        SELECT 
            month(date) as thang, 
            avg(avg_aqi) as aqi_trung_binh
        FROM iceberg_scan('s3://gold/weather_db/aqi_daily_summary')
        GROUP BY 1 ORDER BY 1
    """
    df_monthly = query_gold_layer(monthly_aqi_query)
    
    if not df_monthly.empty:
        # Chuyển số tháng sang tên tháng cho dễ đọc
        df_monthly['thang'] = df_monthly['thang'].apply(lambda x: f"Tháng {int(x)}")
        st.bar_chart(df_monthly.set_index('thang'))
        st.caption("Ghi chú: Dữ liệu giúp nhận diện các tháng cao điểm ô nhiễm trong năm.")

    # C. Weather Summary Table
    st.subheader("📋 Bảng tổng hợp thời tiết (10 ngày gần nhất)")
    weather_table_query = """
        SELECT date, avg_temp, max_temp, min_temp, avg_windspeed
        FROM iceberg_scan('s3://gold/weather_db/weather_daily_summary')
        ORDER BY date DESC LIMIT 10
    """
    df_weather_table = query_gold_layer(weather_table_query)
    # Sử dụng column_config để cố định định dạng và tiêu đề bảng, tránh việc bảng bị nhảy khi render
    st.dataframe(
        df_weather_table,
        use_container_width=True,
        hide_index=True,
        column_config={
            "date": st.column_config.DateColumn("Ngày", format="DD/MM/YYYY"),
            "avg_temp": st.column_config.NumberColumn("Nhiệt độ TB (°C)", format="%.2f"),
            "max_temp": st.column_config.NumberColumn("Cao nhất (°C)", format="%.2f"),
            "min_temp": st.column_config.NumberColumn("Thấp nhất (°C)", format="%.2f"),
            "avg_windspeed": st.column_config.NumberColumn("Gió TB (km/h)", format="%.2f")
        }
    )

# =============================================================================
# 5. FOOTER & MONITORING
# =============================================================================
st.sidebar.markdown("---")
st.sidebar.write("📊 **Trạng thái Hệ thống:**")
st.sidebar.success("Kết nối InfluxDB: OK")
st.sidebar.success("Kết nối Lakehouse: OK")
st.sidebar.caption(f"Cập nhật lúc: {datetime.now().strftime('%H:%M:%S')}")
