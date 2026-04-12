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
logger = logging.getLogger("AQISilverJob")

def run_aqi_silver_transformation(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """
    Spark Job: Bronze AQI JSON -> Silver Iceberg (Cleaned & Deduplicated).
    
    Args:
        start_date (Optional[str]): Ngày bắt đầu (YYYY-MM-DD). Mặc định là ngày hôm qua.
        end_date (Optional[str]): Ngày kết thúc (YYYY-MM-DD). Mặc định bằng start_date.
    
    Workflow:
    1. Khởi tạo SparkSession tích hợp Iceberg và S3A (MinIO).
    2. Xác định danh sách đường dẫn Bronze dựa trên tham số ngày.
    3. Đọc dữ liệu JSON và thực hiện kỹ thuật Flattening (arrays_zip + explode).
    4. Làm sạch, ép kiểu các chỉ số PM2.5, PM10.
    5. Thực hiện MERGE INTO vào bảng Iceberg 'aqi_silver'.
    """

    # Khởi tạo Spark Session (cấu hình giống spark_silver_weather.py)
    spark = SparkSession.builder \
        .appName("AQI_Bronze_To_Silver") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.silver_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.silver_catalog.type", "hadoop") \
        .config("spark.sql.catalog.silver_catalog.warehouse", "s3a://silver") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.defaultCatalog", "silver_catalog") \
        .config("spark.driver.memory", "4g") \
        .config("spark.driver.memoryOverhead", "512m") \
        .config("spark.local.dir", "/opt/airflow/spark_temp") \
        .getOrCreate()

    logger.info("Đã khởi tạo Spark Session cho luồng AQI Silver.")

    # =========================================================================
    # BƯỚC 1: XÁC ĐỊNH PHẠM VI DỮ LIỆU CẦN ĐỌC
    # =========================================================================
    if start_date:
        processing_start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        processing_end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else processing_start_dt
    else:
        yesterday = datetime.now() - timedelta(days=1)
        processing_start_dt = yesterday
        processing_end_dt = yesterday

    # Xây dựng list đường dẫn (Wildcard theo ngày)
    bronze_paths = []
    current_dt = processing_start_dt
    while current_dt <= processing_end_dt:
        path = (
            f"s3a://bronze/aqi/year={current_dt.year}/month={current_dt.month:02d}/"
            f"day={current_dt.day:02d}/*.json"
        )
        bronze_paths.append(path)
        current_dt += timedelta(days=1)

    if not bronze_paths:
        logger.warning("Không tìm thấy đường dẫn dữ liệu AQI hợp lệ.")
        spark.stop()
        return

    try:
        # =========================================================================
        # BƯỚC 2: ĐỌC VÀ XỬ LÝ FLATTENING
        # =========================================================================
        df_raw = spark.read.json(*bronze_paths)

        if df_raw.rdd.isEmpty():
            logger.warning("Dữ liệu Bronze AQI trống. Job dừng.")
            return

        # Open-Meteo Air Quality trả về dữ liệu trong node 'hourly'
        target_node = "hourly"
        
        # 2.1. Zipping: Ghép các mảng PM2.5 và PM10 theo index thời gian
        df_zipped = df_raw.withColumn(
            "zipped_data",
            F.arrays_zip(
                F.col(f"{target_node}.time"),  # Index 0
                F.col(f"{target_node}.pm2_5"), # Index 1
                F.col(f"{target_node}.pm10")   # Index 2
            )
        )

        # 2.2. Exploding: Nổ mảng thành các hàng (rows) độc lập
        df_exploded = df_zipped.select(
            F.lit("HaNoi").alias("station_id"),
            F.explode("zipped_data").alias("item")
        )

        # 2.3. Clean & Cast: Ép kiểu và bóc tách từ struct 'item'
        df_cleaned = df_exploded.select(
            F.col("station_id"),
            # Định dạng ISO8601 từ API: yyyy-MM-dd'T'HH:mm
            F.to_timestamp(F.col("item.time"), "yyyy-MM-dd'T'HH:mm").alias("timestamp"),
            F.col("item.pm2_5").cast("float").alias("pm2_5"),
            F.col("item.pm10").cast("float").alias("pm10")
        ).withColumn(
            # Chỉ số aqi được quyết định dựa trên max(pm2_5, pm10).
            # F.greatest trả về giá trị lớn nhất trong các cột được liệt kê.
            "aqi", F.greatest(F.col("pm2_5"), F.col("pm10"))
        )

        # =========================================================================
        # BƯỚC 3: LỌC DỮ LIỆU & CHUẨN BỊ GHI
        # =========================================================================
        # Chỉ giữ lại dữ liệu lịch sử (<= thời điểm hiện tại)
        # Điều này loại bỏ các dữ liệu dự báo (Forecast) có trong API response
        df_final = df_cleaned.filter(F.col("timestamp") <= F.current_timestamp())

        # Khởi tạo DB và Table Iceberg cho AQI
        spark.sql("CREATE DATABASE IF NOT EXISTS weather_db")
        
        # Partition theo ngày (days) để tối ưu truy vấn theo thời gian
        spark.sql("""
            CREATE TABLE IF NOT EXISTS weather_db.aqi_silver (
                station_id STRING,
                timestamp TIMESTAMP,
                pm2_5 FLOAT,
                pm10 FLOAT,
                aqi FLOAT
            ) USING iceberg
            PARTITIONED BY (days(timestamp))
        """)

        # Đăng ký View tạm để thực hiện MERGE
        df_final.createOrReplaceTempView("staged_aqi")

        # =========================================================================
        # BƯỚC 4: MERGE INTO (UPSERT) - ĐẢM BẢO TÍNH LŨY ĐẲNG
        # =========================================================================
        # Khóa chính để định danh bản ghi duy nhất: station_id + timestamp
        spark.sql("""
            MERGE INTO weather_db.aqi_silver t
            USING staged_aqi s
            ON t.station_id = s.station_id AND t.timestamp = s.timestamp
            WHEN MATCHED THEN
                UPDATE SET
                    t.pm2_5 = s.pm2_5,
                    t.pm10 = s.pm10,
                    t.aqi = s.aqi
            WHEN NOT MATCHED THEN
                INSERT *
        """)

        logger.info(f"Đã hoàn thành MERGE INTO cho dữ liệu AQI vào Silver Layer.")

    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý Spark AQI: {str(e)}")
        raise e
    finally:
        spark.stop()
        logger.info("Spark Session đã được đóng an toàn.")

if __name__ == "__main__":
    # =========================================================================
    # CẤU HÌNH NHẬN THAM SỐ TỪ AIRFLOW
    # =========================================================================
    parser = argparse.ArgumentParser(description="Spark Silver Layer Transformation for Air Quality Data")
    
    # --start-date: Airflow truyền vào qua task instance context (vd: {{ ds }})
    parser.add_argument(
        "--start-date", help="Ngày bắt đầu xử lý (YYYY-MM-DD). Nếu trống, mặc định xử lý dữ liệu hôm qua.")
    
    # --end-date: Dùng cho các tác vụ Backfill hoặc Initial Load dài hạn
    parser.add_argument("--end-date", help="Ngày kết thúc xử lý (YYYY-MM-DD).")
    
    args = parser.parse_args()

    # Thực thi job
    run_aqi_silver_transformation(start_date=args.start_date, end_date=args.end_date)


"""
GHI CHÚ VỀ LOGIC:
1. Dữ liệu AQI từ Open-Meteo trả về theo mảng (Array) tương tự Weather, do đó logic arrays_zip là tối ưu nhất.
2. Chúng ta lưu PM2.5 và PM10 ở tầng Silver. Việc tính toán US-AQI index cụ thể sẽ được thực hiện ở tầng Gold.
3. Iceberg tự động quản lý việc ghi file Parquet xuống MinIO và cập nhật metadata.
"""