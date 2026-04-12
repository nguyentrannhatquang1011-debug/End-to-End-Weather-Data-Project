import os
import logging
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse
from datetime import datetime, timedelta

# =============================================================================
# CẤU HÌNH LOGGING
# =============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("GoldAggregationJob")

def run_gold_aggregation(target_type: str, start_date: Optional[str] = None, end_date: Optional[str] = None):
    """
    Spark Job: Silver Iceberg -> Gold Iceberg (Pre-aggregated Summary).
    
    Args:
        target_type (str): Loại dữ liệu cần tổng hợp ('weather' hoặc 'aqi').
        start_date (Optional[str]): Ngày bắt đầu xử lý (YYYY-MM-DD).
        end_date (Optional[str]): Ngày kết thúc xử lý (YYYY-MM-DD).

    Workflow:
    1. Khởi tạo SparkSession kết nối Iceberg Catalog.
    2. Đọc dữ liệu từ bảng Silver tương ứng.
    3. Thực hiện GroupBy theo 'station_id' và 'date'.
    4. Tính toán các chỉ số thống kê (Avg, Max, Min).
    5. MERGE INTO vào bảng Gold để đảm bảo tính lũy đẳng.
    """

    # Khởi tạo Spark Session tối ưu cho Iceberg
    spark = SparkSession.builder \
        .appName(f"Gold_Aggregation_{target_type}") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.gold_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.gold_catalog.type", "hadoop") \
        .config("spark.sql.catalog.gold_catalog.warehouse", "s3a://gold") \
        .config("spark.sql.catalog.silver_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.silver_catalog.type", "hadoop") \
        .config("spark.sql.catalog.silver_catalog.warehouse", "s3a://silver") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.defaultCatalog", "gold_catalog") \
        .config("spark.driver.memory", "4g") \
        .config("spark.driver.memoryOverhead", "512m") \
        .config("spark.local.dir", "/opt/airflow/spark_temp") \
        .getOrCreate()

    logger.info(f"Bắt đầu quy trình tổng hợp Gold Layer cho: {target_type}")

    # =========================================================================
    # XÁC ĐỊNH PHẠM VI DỮ LIỆU XỬ LÝ (DATE RANGE RESOLUTION)
    # =========================================================================
    # Sử dụng datetime.strptime để parse chuỗi đầu vào. 
    # Việc này giúp đảm bảo start_date và end_date luôn đúng định dạng YYYY-MM-DD.
    
    try:
        if start_date and start_date.strip():
            # Parse start_date từ tham số truyền vào
            start_dt_obj = datetime.strptime(start_date.strip(), "%Y-%m-%d")
        else:
            # Mặc định xử lý ngày hôm qua cho luồng Daily Batch
            start_dt_obj = datetime.now() - timedelta(days=1)

        if end_date and end_date.strip():
            # Parse end_date từ tham số truyền vào
            end_dt_obj = datetime.strptime(end_date.strip(), "%Y-%m-%d")
        else:
            # Nếu không có end_date, mặc định bằng start_date (xử lý 1 ngày duy nhất)
            end_dt_obj = start_dt_obj
            
        # Chuyển đổi ngược lại thành chuỗi để sử dụng trong Spark SQL Filter
        st_dt = start_dt_obj.strftime("%Y-%m-%d")
        en_dt = end_dt_obj.strftime("%Y-%m-%d")
        
    except ValueError as ve:
        logger.error(f"Định dạng ngày tháng không hợp lệ (Phải là YYYY-MM-DD): {ve}")
        raise ve

    logger.info(f">>> Phạm vi thực hiện tổng hợp Gold: {st_dt} đến {en_dt}")

    try:
        if target_type == "weather":
            # =================================================================
            # LOGIC TỔNG HỢP DỮ LIỆU THỜI TIẾT
            # =================================================================
            # Đọc từ Silver
            df_silver = spark.table("silver_catalog.weather_db.weather_silver") \
                .filter(F.to_date("timestamp").between(st_dt, en_dt))

            # Tính toán Pre-aggregation theo ngày
            df_gold = df_silver.groupBy("station_id", F.to_date("timestamp").alias("date")) \
                .agg(
                    F.round(F.avg("temperature"), 2).alias("avg_temp"),
                    F.max("temperature").alias("max_temp"),
                    F.min("temperature").alias("min_temp"),
                    F.round(F.avg("windspeed"), 2).alias("avg_windspeed"),
                    # Sử dụng F.mode (Spark 3.4+) để lấy mã thời tiết xuất hiện nhiều nhất (Dominant).
                    # Không dùng F.avg vì trung bình của "Nắng" và "Mưa" không có ý nghĩa.
                    F.mode("weathercode").alias("dominant_weather_code"),
                    # Sử dụng F.mode cho hướng gió để tránh sai số toán học khi tính trung bình góc (vd: 350 độ và 10 độ).
                    F.mode("winddirection").alias("dominant_wind_direction")
                )

            # Khởi tạo bảng Gold nếu chưa tồn tại
            spark.sql("CREATE DATABASE IF NOT EXISTS gold_catalog.weather_db")
            spark.sql("""
                CREATE TABLE IF NOT EXISTS gold_catalog.weather_db.weather_daily_summary (
                    station_id STRING,
                    date DATE,
                    avg_temp FLOAT,
                    max_temp FLOAT,
                    min_temp FLOAT,
                    avg_windspeed FLOAT,
                    dominant_weather_code INT,
                    dominant_wind_direction FLOAT
                ) USING iceberg
                PARTITIONED BY (years(date))
            """)

            # Định nghĩa bảng đích và logic merge cho Weather
            target_table = "gold_catalog.weather_db.weather_daily_summary"
            merge_logic = """
                t.avg_temp = s.avg_temp,
                t.max_temp = s.max_temp,
                t.min_temp = s.min_temp,
                t.avg_windspeed = s.avg_windspeed,
                t.dominant_weather_code = s.dominant_weather_code,
                t.dominant_wind_direction = s.dominant_wind_direction
            """

        elif target_type == "aqi":
            # =================================================================
            # LOGIC TỔNG HỢP DỮ LIỆU AQI
            # =================================================================
            # Đọc từ Silver_catalog, lọc theo khoảng thời gian được chỉ định.
            df_silver = spark.table("silver_catalog.weather_db.aqi_silver") \
                .filter(F.to_date("timestamp").between(st_dt, en_dt))

            df_gold = df_silver.groupBy("station_id", F.to_date("timestamp").alias("date")) \
                .agg(
                    F.round(F.avg("aqi"), 2).alias("avg_aqi"),
                    F.max("aqi").alias("max_aqi"),
                    F.round(F.avg("pm2_5"), 2).alias("avg_pm2_5"),
                    F.round(F.avg("pm10"), 2).alias("avg_pm10")
                )

            # Dữ liệu đã được tổng hợp theo ngày, nên trường timestamp gốc không còn ý nghĩa. 
            #       Thay vào đó, chúng ta dùng 'date' để định danh ngày tổng hợp.
            spark.sql("CREATE DATABASE IF NOT EXISTS gold_catalog.weather_db")
            spark.sql("""
                CREATE TABLE IF NOT EXISTS gold_catalog.weather_db.aqi_daily_summary (
                    station_id STRING,
                    date DATE, 
                    avg_aqi FLOAT,
                    max_aqi FLOAT,
                    avg_pm2_5 FLOAT,
                    avg_pm10 FLOAT
                ) USING iceberg
                PARTITIONED BY (years(date))
            """)

            # Định nghĩa bảng đích và logic merge cho AQI
            target_table = "gold_catalog.weather_db.aqi_daily_summary"
            merge_logic = """
                t.avg_aqi = s.avg_aqi,
                t.max_aqi = s.max_aqi,
                t.avg_pm2_5 = s.avg_pm2_5,
                t.avg_pm10 = s.avg_pm10
            """
        else:
            raise ValueError(f"Không hỗ trợ target_type: {target_type}")

        # =====================================================================
        # THỰC THI MERGE INTO (UPSERT)
        # =====================================================================
        # Đăng ký view tạm để chạy SQL
        df_gold.createOrReplaceTempView("staged_gold")

        spark.sql(f"""
            MERGE INTO {target_table} t
            USING staged_gold s
            ON t.station_id = s.station_id AND t.date = s.date
            WHEN MATCHED THEN
                UPDATE SET {merge_logic}
            WHEN NOT MATCHED THEN
                INSERT *
        """)

        logger.info(f"Hoàn thành Pre-aggregation cho {target_table} từ {st_dt} đến {en_dt}.")

    except Exception as e:
        logger.error(f"Lỗi khi thực hiện Gold Job cho {target_type}: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    # =========================================================================
    # QUẢN LÝ THAM SỐ DÒNG LỆNH
    # =========================================================================
    parser = argparse.ArgumentParser(description="Spark Gold Layer - Pre-aggregation Job")
    
    # Bỏ 'required=True' cho phép script tự chạy toàn bộ luồng nếu không truyền tham số.
    parser.add_argument(
        "--type", 
        choices=["weather", "aqi", "all"], 
        default="all",
        help="Loại dữ liệu cần tổng hợp (mặc định: all)."
    )
    parser.add_argument("--start-date", help="Ngày bắt đầu (YYYY-MM-DD).")
    parser.add_argument("--end-date", help="Ngày kết thúc (YYYY-MM-DD).")

    args = parser.parse_args()


    if args.type == "all":
        # Nếu chọn 'all', script sẽ thực hiện tổng hợp lần lượt cho cả Weather và AQI.
        # Chạy tuần tự (Sequential) giúp giải phóng RAM sau mỗi lần đóng Spark Session,
        # tránh việc xử lý song song gây quá tải cho tài nguyên 16GB của máy.
        for t in ["weather", "aqi"]:
            run_gold_aggregation(
                target_type=t, 
                start_date=args.start_date, 
                end_date=args.end_date
            )
    else:
        # Chạy đơn lẻ cho loại dữ liệu được chỉ định (Weather hoặc AQI)
        run_gold_aggregation(
            target_type=args.type, 
            start_date=args.start_date, 
            end_date=args.end_date
        )

"""
GIẢI THÍCH CHI TIẾT WORKFLOW:

1. Tại sao dùng Gold Layer? 
   Bảng Silver chứa dữ liệu chi tiết theo từng 15 phút. Nếu dashboard vẽ biểu đồ theo tháng, 
   Spark/DuckDB phải quét hàng nghìn file Parquet. Bảng Gold tổng hợp sẵn theo ngày 
   giúp giảm số lượng bản ghi xuống 1/96 (với dữ liệu 15 phút), cực kỳ tiết kiệm RAM.

2. Logic Idempotency:
   Sử dụng MERGE INTO dựa trên (station_id + date). Nếu bạn chạy lại job cho một ngày 
   đã có dữ liệu (do dữ liệu Silver được bổ sung muộn), kết quả Gold sẽ được cập nhật mới nhất.

3. Partitioning:
   Tại Gold Layer, dữ liệu đã rất gọn, nên chúng ta phân vùng theo years(date) 
   thay vì days(date) để tránh tạo ra quá nhiều thư mục nhỏ (Small File Problem).
"""