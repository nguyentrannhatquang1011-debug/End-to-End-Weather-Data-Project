import os
import argparse
import logging
import json
import pandas as pd
import duckdb
import requests
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from typing import Dict, Any, List, Optional

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

from data_transformer import map_silver_to_influx_weather, map_silver_to_influx_aqi

# Tải biến môi trường
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("StreamingBackfiller")

class StreamingBackfiller:
    """Lớp xử lý hồi phục dữ liệu (Backfill) cho InfluxDB từ Lakehouse hoặc API."""

    def __init__(self) -> None:
        """Khởi tạo kết nối tới các dịch vụ InfluxDB và cấu hình DuckDB."""
        # InfluxDB Config
        self.influx_url = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
        self.influx_token = os.getenv("INFLUXDB_TOKEN")
        self.influx_org = os.getenv("INFLUXDB_ORG", "weather_org")
        self.influx_bucket = os.getenv("INFLUXDB_BUCKET", "weather_data")

        # Tối ưu hóa Connection Pooling cho API Calls
        self.http_session = requests.Session()
        adapter = HTTPAdapter(pool_connections=5, pool_maxsize=10)
        self.http_session.mount("https://", adapter)
        self.http_session.mount("http://", adapter)

        # MinIO/S3 Config cho DuckDB
        self.s3_user = os.getenv("MINIO_ROOT_USER")
        self.s3_pass = os.getenv("MINIO_ROOT_PASSWORD")
        self.s3_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", "")

        # Khởi tạo InfluxDB Client
        self.influx_client = InfluxDBClient(url=self.influx_url, token=self.influx_token, org=self.influx_org)
        self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)

        # Khởi tạo DuckDB để scan Iceberg
        self.duck_conn = duckdb.connect(database=':memory:')
        # Chỉ định thư mục extension cố định để tránh lỗi phân quyền hoặc network trong Docker
        self.duck_conn.execute("SET extension_directory='/opt/airflow/duckdb_extensions';")
        # Load extension (Dockerfile đã cài đặt sẵn để đảm bảo ổn định)
        self.duck_conn.execute("LOAD httpfs;")
        self.duck_conn.execute("LOAD iceberg;")
        
        self.duck_conn.execute(f"SET s3_endpoint='{self.s3_endpoint}';")
        self.duck_conn.execute(f"SET s3_access_key_id='{self.s3_user}';")
        self.duck_conn.execute(f"SET s3_secret_access_key='{self.s3_pass}';")
        self.duck_conn.execute("SET s3_use_ssl=false; SET s3_url_style='path';")

    def _filter_forecast(self, df: pd.DataFrame, time_col: str) -> pd.DataFrame:
        """Loại bỏ dữ liệu dự báo (Forecast) - Chỉ giữ lại dữ liệu <= hiện tại."""
        # Đảm bảo so sánh giữa hai đối tượng cùng múi giờ (aware vs aware)
        now = pd.Timestamp.now(tz='Asia/Bangkok')
        
        # Ép kiểu và chuẩn hóa múi giờ cho DataFrame
        df[time_col] = pd.to_datetime(df[time_col])
        if df[time_col].dt.tz is None:
            df[time_col] = df[time_col].dt.tz_localize('UTC').dt.tz_convert('Asia/Bangkok')
        else:
            df[time_col] = df[time_col].dt.tz_convert('Asia/Bangkok')
            
        return df[df[time_col] <= now]

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def fetch_from_api(self, data_type: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Lấy dữ liệu từ API Open-Meteo trong trường hợp dữ liệu chưa có trong Lakehouse."""
        logger.info(f"⚡ Gọi API Open-Meteo để lấy bù gap cho {data_type} ({start_date} -> {end_date})...")
        
        # Xác định endpoint: Nếu ngày start là quá khứ (> 1 ngày trước), dùng Archive API
        is_historical = datetime.strptime(start_date, "%Y-%m-%d") < (datetime.now() - timedelta(days=1))
        
        if data_type == "weather":
            url = "https://archive-api.open-meteo.com/v1/archive" if is_historical else "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": 21.0285, "longitude": 105.8542,
                "start_date": start_date, "end_date": end_date,
                "hourly": "temperature_2m,wind_speed_10m,wind_direction_10m,weather_code",
                "timezone": "Asia/Bangkok"
            }
            node = "hourly"
        else:  # aqi
            # Open-Meteo Air Quality API dùng chung endpoint cho cả history và forecast
            url = "https://air-quality-api.open-meteo.com/v1/air-quality"
            params = {
                "latitude": 21.0285, "longitude": 105.8542,
                "start_date": start_date, "end_date": end_date,
                "hourly": "pm2_5,pm10", "timezone": "Asia/Bangkok"
            }
            node = "hourly"

        # Sử dụng session đã cấu hình pooling thay vì gọi trực tiếp requests.get
        response = self.http_session.get(url, params=params, timeout=20)
        response.raise_for_status()
        data = response.json().get(node, {})
        
        if not data: return pd.DataFrame()
        
        df = pd.DataFrame(data)
        # Gán station_id mặc định để khớp với schema của tầng Silver và tránh giá trị 'nan' trong InfluxDB tags
        df['station_id'] = 'HaNoi'
        df.rename(columns={"time": "timestamp", "temperature_2m": "temperature", 
                           "wind_speed_10m": "windspeed", "wind_direction_10m": "winddirection",
                           "weather_code": "weathercode"}, inplace=True)
        
        if data_type == "aqi":
            # STRICT SCHEMA: Tính toán AQI từ pm2_5 và pm10 để khớp 100% với logic 'greatest' ở spark_silver_aqi.py
            # Không lấy cột 'aqi' có sẵn của API vì nó có thể dùng scale khác.
            df['aqi'] = df[['pm2_5', 'pm10']].max(axis=1)
            # Giải thích .max(axis=1): Tính giá trị lớn nhất giữa pm2_5 và pm10 cho mỗi hàng (theo từng timestamp).
            #       axis=1 có nghĩa là chúng ta đang áp dụng hàm max theo hàng (row-wise)
            # Điều này đảm bảo rằng chúng ta tuân thủ đúng logic 'greatest' đã áp dụng trong spark_silver_aqi.py.
            
        return df

    def fetch_from_iceberg(self, data_type: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Truy vấn dữ liệu từ Silver Layer (Iceberg) trên MinIO."""
        table_name = "weather_silver" if data_type == "weather" else "aqi_silver"
        s3_path = f"s3://silver/weather_db/{table_name}"
        
        query = f"""
            SELECT * FROM iceberg_scan('{s3_path}')
            WHERE CAST(timestamp AS DATE) BETWEEN '{start_date}' AND '{end_date}'
        """
        try:
            logger.info(f"Đang quét Lakehouse (SSOT) cho {data_type}...")
            return self.duck_conn.execute(query).df()
        except Exception as e:
            logger.warning(f"Không thể đọc từ Lakehouse (Có thể bảng chưa tồn tại): {e}")
            return pd.DataFrame()

    def write_to_influx(self, df: pd.DataFrame, data_type: str) -> None:
        """Chuyển đổi DataFrame thành InfluxDB Points và ghi đồng bộ."""
        if df.empty:
            logger.info("Không có dữ liệu để ghi vào InfluxDB.")
            return

        # Lọc bỏ forecast
        df = self._filter_forecast(df, "timestamp")

        # --- TỐI ƯU: Lọc theo Retention Policy (Tránh lỗi 422 Unprocessable Entity) ---
        # InfluxDB trong project được cấu hình 30 ngày (720h).
        # Ta tính toán mốc giới hạn (Lower Bound) dựa trên thời điểm hiện tại.
        retention_limit = pd.Timestamp.now(tz='Asia/Bangkok') - pd.Timedelta(days=30)

        initial_count = len(df)
        df = df[df['timestamp'] >= retention_limit]
        dropped_count = initial_count - len(df)

        if dropped_count > 0:
            logger.warning(f"Bỏ qua {dropped_count} bản ghi cũ hơn giới hạn Retention Policy (30 ngày).")

        if df.empty:
            logger.info("Không còn dữ liệu hợp lệ nằm trong phạm vi Retention Policy để ghi.")
            return

        points = []
        
        for _, row in df.iterrows():
            # Chuyển row thành dict để đưa vào transformer
            row_dict = row.to_dict()

            if data_type == "weather":
                m = map_silver_to_influx_weather(row_dict)
            else:
                m = map_silver_to_influx_aqi(row_dict)
            
            p = Point(m["measurement"])
            for k, v in m["tags"].items(): p.tag(k, v)
            for k, v in m["fields"].items(): p.field(k, v)
            p.time(m["time"])
            points.append(p)

        if points:
            self.write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=points)
            logger.info(f"Đã ghi bù {len(points)} bản ghi {data_type} vào InfluxDB.")

    def run_backfill(self, data_type: str, start_date: str, end_date: str) -> None:
        """Workflow chính: Kiểm tra Lakehouse trước, nếu thiếu gọi API."""
        # 1. Lấy dữ liệu hiện có từ Lakehouse
        df = self.fetch_from_iceberg(data_type, start_date, end_date)
        
        # ÉP KIỂU VỀ MÚI GIỜ HÀ NỘI NGAY LẬP TỨC
        if not df.empty:
            # pd.to_datetime(..., utc=True) giúp nhận diện múi giờ từ Iceberg (thường là UTC) 
            # sau đó convert sang Asia/Bangkok
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True).dt.tz_convert('Asia/Bangkok')

        # 2. KIỂM TRA GAP (THIẾU DỮ LIỆU)
        # Tính số giờ dự kiến: (số ngày + 1) * 24 giờ
        d1 = datetime.strptime(start_date, "%Y-%m-%d")
        d2 = datetime.strptime(end_date, "%Y-%m-%d")
        expected_hours = ((d2 - d1).days + 1) * 24
        today_str = datetime.now().strftime("%Y-%m-%d")

        # Logic bù Gap:
        # - Nếu DataFrame rỗng
        # - Hoặc số lượng bản ghi < 90% số giờ dự kiến (có gap lớn trong ngày cũ)
        # - Hoặc ngày kết thúc là ngày hôm nay (dữ liệu Silver chưa kịp có)
        if df.empty or len(df) < (expected_hours * 0.9) or end_date >= today_str:
            logger.info(f"🔍 Phát hiện thiếu dữ liệu (Hiện có: {len(df)}/{expected_hours}h). Đang bù gap từ API...")
            api_df = self.fetch_from_api(data_type, start_date, end_date)
            
            if not api_df.empty:
                # API Open-Meteo trả về naive timestamp (không múi giờ) nhưng thực tế là giờ địa phương.
                api_df['timestamp'] = pd.to_datetime(api_df['timestamp']).dt.tz_localize('Asia/Bangkok')
            
            if not df.empty:
                # Gộp hai nguồn, ưu tiên API (keep='last') cho dữ liệu trùng.
                df = pd.concat([df, api_df]).drop_duplicates(subset=['timestamp'], keep='last')
                # Giải thích cách thức hoạt động của drop_duplicates:
                # - subset=['timestamp']: Chỉ xem xét cột 'timestamp' để xác định bản ghi trùng lặp.
                # - keep='last': Nếu có nhiều bản ghi cùng timestamp, giữ lại bản ghi cuối cùng (trong trường hợp này là bản ghi từ API).
                # Giải thích bản ghi cuối cùng, cùng timestamp là như thế nào:

                # Có phải có nghĩa là thứ tự ghinh vào DataFrame sẽ quyết định bản ghi nào được giữ lại không?
                # - Đúng vậy, khi chúng ta concat df (Lakehouse) và api_df (API), nếu có cùng timestamp, bản ghi nào được giữ lại sẽ phụ thuộc vào thứ tự của chúng trong DataFrame sau khi concat.
                # - Với keep='last', nếu có cùng timestamp, bản ghi từ api_df sẽ được giữ lại vì nó được thêm vào sau df trong quá trình concat. 
                # - Điều này có nghĩa là nếu có dữ liệu từ API cho cùng một timestamp, nó sẽ ghi đè lên dữ liệu từ Lakehouse cho timestamp đó.
            else:
                df = api_df

        # 3. Ghi vào InfluxDB
        # Đảm bảo sắp xếp theo thời gian trước khi ghi
        df = df.sort_values(by='timestamp')
        self.write_to_influx(df, data_type)

    def close(self) -> None:
        """Giải phóng tài nguyên."""
        self.influx_client.close()
        if hasattr(self, 'http_session'):
            self.http_session.close()
        self.duck_conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Streaming Backfill Script (Lakehouse to InfluxDB)")
    parser.add_argument("--type", choices=["weather", "aqi"], required=True)
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    
    args = parser.parse_args()
    
    backfiller = StreamingBackfiller()
    try:
        backfiller.run_backfill(args.type, args.start_date, args.end_date)
    finally:
        backfiller.close()