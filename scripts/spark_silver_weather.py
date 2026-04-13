import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F # Import module functions của Spark
from typing import Optional
import argparse # Import module để xử lý tham số dòng lệnh
from datetime import datetime, timedelta # Import module để làm việc với ngày tháng

# =============================================================================
# CẤU HÌNH LOGGING
# =============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("WeatherSilverJob")

def run_weather_silver_transformation(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """
    Spark Job: Bronze JSON -> Silver Iceberg (Cleaned & Deduplicated).
    
    Args:
        start_date (Optional[str]): Ngày bắt đầu (YYYY-MM-DD) để xử lý dữ liệu từ Bronze.
                                     Nếu không có, mặc định là ngày hôm qua.
        end_date (Optional[str]): Ngày kết thúc (YYYY-MM-DD) để xử lý dữ liệu từ Bronze.
                                   Nếu không có, mặc định là start_date.
    
    Workflow:
    1. Khởi tạo SparkSession với cấu hình Iceberg Catalog và S3A.
    2. Đọc dữ liệu JSON từ cấu trúc partition của Bronze layer.
    3. Xử lý logic Flatten: Đồng bộ hóa các mảng dữ liệu (Time, Temp, Wind,...) thành hàng.
    4. Ép kiểu dữ liệu chuẩn (Timestamp, Float).
    5. Thực hiện MERGE INTO (Upsert) vào bảng Iceberg.
    """
    
    # Khởi tạo Spark Session với cấu hình tối ưu cho Iceberg & MinIO
    # Giải thích chi tiết cấu hình:
    # - spark.jars.packages: Chỉ định các thư viện cần thiết để Spark có thể tích hợp với Iceberg và S3 (MinIO).
    # - spark.driver.extraJavaOptions: Cấu hình logging cho Spark Driver để ghi log ra file, giúp debug dễ dàng hơn.
    # - spark.ui.showConsoleProgress: Tắt hiển thị progress bar trên console để log rõ ràng hơn trong môi trường batch.
        #.config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        #.config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:/opt/airflow/scripts/log4j.properties") \
        #.config("spark.ui.showConsoleProgress", "false") \

    # - spark.sql.extensions: Kích hoạt Iceberg Spark Extensions để sử dụng các tính năng nâng cao của Iceberg trong Spark SQL.
    # - spark.sql.catalog.silver_catalog: Định nghĩa một Catalog mới tên "silver_catalog" sử dụng SparkCatalog của Iceberg.
    # - spark.sql.catalog.silver_catalog.type: Chỉ định loại Catalog là "hadoop" để sử dụng hệ thống file Hadoop (ở đây là S3A).
    # - spark.sql.catalog.silver_catalog.warehouse: Đường dẫn đến thư mục giữ dữ liệu của Catalog trên S3 (ở đây là bucket "silver").

    # - spark.hadoop.fs.s3a.endpoint: Địa chỉ endpoint của MinIO để Spark có thể kết nối.
    # - spark.hadoop.fs.s3a.access.key & secret.key: Thông tin xác thực để Spark có thể truy cập MinIO.
    # - spark.hadoop.fs.s3a.path.style.access: Bật chế độ truy cập theo kiểu path-style (thay vì virtual-hosted style) để tương thích với MinIO.
    # - spark.hadoop.fs.s3a.impl: Chỉ định lớp triển khai S3AFileSystem để Spark biết cách tương tác với S3.
    
    # - spark.sql.defaultCatalog: Đặt "silver_catalog" làm Catalog mặc định để khi tạo bảng không cần chỉ định Catalog.
    
    # - .config("spark.local.dir", "/tmp/spark-temp") \
    # Cấu hình thư mục tạm (Scratch Space) trỏ đến Host Volume đã mount trong docker-compose.
        # Điều này đảm bảo các file shuffle/temp không lưu trong container layer,
        # giúp tối ưu hóa hiệu năng I/O và quản lý dung lượng ổ đĩa trên Windows/WSL2.
    spark = SparkSession.builder \
        .appName("Weather_Bronze_To_Silver") \
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
    
    # Giới hạn RAM trực tiếp trong code để bảo vệ tài nguyên máy 16GB.
    # Lưu ý: Nếu chạy qua spark-submit trong DAG, tham số dòng lệnh Bash trong Airflow sẽ được ưu tiên.
    logger.info("Đã khởi tạo Spark Session thành công.")

    # =========================================================================
    # Bước 1. XÁC ĐỊNH PHẠM VI DỮ LIỆU CẦN XỬ LÝ TỪ BRONZE LAYER
    # Dựa vào tham số start_date và end_date được truyền vào từ Airflow DAG.
    # Nếu không có tham số, mặc định sẽ xử lý dữ liệu của ngày hôm qua (phù hợp cho Daily Batch).
    
    processing_start_dt = None
    processing_end_dt = None

    if start_date:
        processing_start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        if end_date:
            processing_end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            processing_end_dt = processing_start_dt # Nếu chỉ có start_date, xử lý một ngày duy nhất
    else:
        # Nếu không có tham số ngày, mặc định xử lý dữ liệu của ngày hôm qua.
        # Điều này đảm bảo rằng batch_extractor đã có đủ thời gian để đẩy dữ liệu lên Bronze.
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        processing_start_dt = yesterday
        processing_end_dt = yesterday
        logger.info(f"Không có tham số ngày được cung cấp. Mặc định xử lý dữ liệu cho ngày: {yesterday.strftime('%Y-%m-%d')}")

    # Xây dựng danh sách các đường dẫn Bronze cần đọc
    # # Vì sao lại là list mà không phải là một đường dẫn duy nhất? 
    #   - Vì Spark có thể nhận nhiều đường dẫn cùng lúc, giúp chúng ta xử lý dữ liệu của nhiều ngày trong một lần chạy nếu cần.
    #   - Nếu Airflow DAG được cấu hình để chạy hàng ngày với một ngày cụ thể, chúng ta sẽ chỉ có một đường dẫn.
    #   - Nhưng nếu chúng ta muốn chạy một lần cho cả tháng, chúng ta có thể truyền nhiều ngày và Spark sẽ tự động quét tất cả các đường dẫn đó.
    
    # Tính toán số ngày cần xử lý
    diff_days = (processing_end_dt - processing_start_dt).days

    if diff_days > 30:
        # Chế độ Initial Load: Sử dụng Wildcard toàn cục (Globbing).
        # Điều này giúp Spark tự tìm các file tồn tại mà không bị crash khi gặp folder trống (lỗi PATH_NOT_FOUND).
        bronze_paths = ["s3a://bronze/weather/year=*/month=*/day=*/*.json"]
        logger.info("Phát hiện Initial Load: Sử dụng wildcard pattern để quét toàn bộ Bronze Layer.")
    else:
        # Chế độ Daily/Backfill: Sử dụng đường dẫn cụ thể để tối ưu hóa hiệu năng liệt kê tệp của S3.
        bronze_paths = []
        current_dt = processing_start_dt
        while current_dt <= processing_end_dt:
            path = (
                f"s3a://bronze/weather/year={current_dt.year}/month={current_dt.month:02d}/"
                f"day={current_dt.day:02d}/*.json"
            )
            bronze_paths.append(path) 
            current_dt += timedelta(days=1)

    if not bronze_paths:
        logger.warning("Không có đường dẫn Bronze nào được tạo dựa trên phạm vi ngày. Job kết thúc.")
        spark.stop()
        return

    try:
        # ========== Bước 1.1. Đọc dữ liệu JSON từ các đường dẫn Bronze đã được xác định ==========
        # Param: '*bronze_paths' mà không phải là bronze_paths?
        #   - * (splat operator) trong Python được sử dụng để "giảinén" một list thành các đối số riêng biệt khi gọi một hàm.  
        # Truyền trực tiếp list bronze_paths thay vì dùng toán tử giải nén (*).
        # Điều này giúp tránh lỗi vượt quá giới hạn đối số (TypeError) khi xử lý Initial Load nhiều năm (hàng nghìn file).
        df_raw = spark.read.json(bronze_paths)

        # read.json() sẽ tự động infer schema và tạo DataFrame với cấu trúc bảng lồng nhau.
        # Vì sao trên lại là một cấu trúc bảng lồng nhau (nested table structure):
        # - Các cột có tên "minutely_15.time", "minutely_15.temperature_2m",... cho thấy rằng dữ liệu gốc có cấu trúc JSON lồng nhau (nested JSON structure) 
        # - Mỗi cột tương ứng với một trường trong đối tượng JSON gốc, 
        #       và các giá trị trong cột là các mảng (Array) chứa dữ liệu theo từng mốc thời gian (ví dụ: time, temperature, wind_speed,...).

        # Ví dụ minh họa df_raw:
        # station_id | minutely_15.time       | minutely_15.temperature_2m | minutely_15.wind_speed_10m | ...
        # -----------|------------------------|----------------------------|----------------------------|-------
        # HaNoi      | [2024-05-20T00:00, 2024-05-20T00:15, ...] | [25.0, 25.5, ...] | [5.0, 5.5, ...]  
        # - Một hàng duy nhất với các cột chứa mảng dữ liệu.
        # - Cấu trúc này cần được xử lý thêm (flattening) để chuyển đổi thành dạng bảng phẳng (flat table) phù hợp với mô hình dữ liệu của Silver Layer và dễ dàng truy vấn sau này.


        if df_raw.rdd.isEmpty():
            logger.warning("Không tìm thấy dữ liệu trong Bronze bucket. Job kết thúc.")
            return


        # =========================================================================
        # Bước 2. Xác định nguồn dữ liệu (Minutely_15 được ưu tiên hơn Hourly nếu tồn tại)
        has_minutely = "minutely_15" in df_raw.columns # Kiểm tra xem cột "minutely_15" có tồn tại trong DataFrame hay không.
        target_node = "minutely_15" if has_minutely else "hourly"
        logger.info(f"Đang xử lý dữ liệu loại: {target_node}")


        # ===========================================================================
        # Bước 3. Kỹ thuật FLATTENING (Nổ mảng thành hàng)
        # Bước 3.1: Đồng bộ hóa các mảng dữ liệu song song (Zipping)
        # Open-Meteo trả về dữ liệu theo kiểu: time: [T1, T2], temp: [25, 26].
        # arrays_zip sẽ ghép chúng lại thành: zipped_data: [(T1, 25), (T2, 26)].
        # Điều này cực kỳ quan trọng để đảm bảo tính toàn vẹn: thời gian nào đi với giá trị đó.
        df_zipped = df_raw.withColumn( 
            "zipped_data", # Tạo một cột mới "zipped_data" chứa kết quả của việc đồng bộ hóa các mảng dữ liệu song song.
            F.arrays_zip( 
                # F.col(f"{target_node}.time"): Đây chính là lúc bạn đang truy xuất vào dữ liệu đã có sẵn trong df_raw. 
                #  - Bạn đang chỉ định cho Spark biết: "Hãy lấy dữ liệu từ mảng time nằm bên trong struct {target_node}".
                F.col(f"{target_node}.time"),            # Index 0
                F.col(f"{target_node}.temperature_2m"),  # Index 1
                F.col(f"{target_node}.wind_speed_10m"),   # Index 2
                F.col(f"{target_node}.wind_direction_10m"), # Index 3
                F.col(f"{target_node}.weather_code")     # Index 4
            )
        )
        # Nếu API trả về time: [T1, T2], temp: [25, 26], wind_speed: [5, 6], wind_direction: [90, 100], weather_code: [0, 1]
        # Sau khi arrays_zip sẽ thành:
        # zipped_data: [
        #   (T1, 25, 5, 90, 0), - mô tả dòng dữ liệu đầu tiên với thời gian T1, nhiệt độ 25, tốc độ gió 5, hướng gió 90 và mã thời tiết 0.
        #   (T2, 26, 6, 100, 1)
        # ]

        # Ví dụ minh họa df_zipped sẽ có cấu trúc như sau:
        # station_id | zipped_data
        # -----------|----------------------
        # HaNoi      | [(T1, 25, 5, 90, 0), (T2, 26, 6, 100, 1)] 
        # - Một hàng duy nhất với một cột chứa mảng các struct.



        # Bước 3.2: Biến đổi mảng thành các hàng độc lập (Exploding)
        # F.explode sẽ "nổ" array trong zipped_data này ra: nếu array có 24 phần tử, 1 row thô sẽ biến thành 24 hàng.
        # 'item' sẽ là tên cột chứa struct (bản ghi) đã được bóc tách.
        df_exploded = df_zipped.select( # Chọn các cột cần thiết để tạo DataFrame mới
            
            # Vì dữ liệu chỉ có một trạm, chúng ta gán trực tiếp giá trị "HaNoi" cho tất cả các hàng. 
            # - Nếu có nhiều trạm, chúng ta sẽ cần logic phức tạp hơn để xác định station_id dựa trên thông tin trong JSON.
            F.lit("HaNoi").alias("station_id"),

        # Bóc tách mảng đã được zip thành các hàng riêng biệt, mỗi hàng/Row chứa một struct với các trường tương ứng (time, temperature, wind_speed, wind_direction, weather_code) 
        # - sẽ được được truy cập qua item.0, item.1, item.2, item.3, item.4
        # Ví dụ minh họa: Nếu zipped_data có giá trị [(T1, 25, 5, 90, 0), (T2, 26, 6, 100, 1)], sau khi explode sẽ thành:
        # station_id | item
        # -----------|----------------------
        # HaNoi      | (T1, 25, 5, 90, 0)
        # HaNoi      | (T2, 26, 6, 100, 1)
            F.explode("zipped_data").alias("item") 
        )

        # ===== Lý do làm như trên:  Việc sử dụng arrays_zip để "khóa" các phần tử cùng index lại với nhau, sau đó mới explode để tách ra từng hàng


        # Bước 3.3: Bóc tách Struct và ép kiểu (Projection & Casting)
        # Trong Spark 3.5.0, arrays_zip giữ lại tên trường gốc của các cột con.
        # Chúng ta truy cập bằng tên (item.field_name) thay vì index (item.0) để đảm bảo tính chính xác.

        df_cleaned = df_exploded.select( # Chọn các cột cần thiết để tạo DataFrame mới
            F.col("station_id"), # Giữ nguyên station_id đã gán ở bước trước
            # Vì API trả về thời gian ở định dạng ISO8601 thiếu giây và offset, 
            #  - chúng ta sử dụng to_timestamp với format phù hợp để chuyển đổi thành kiểu Timestamp của Spark.
            F.to_timestamp(F.col("item.time"), "yyyy-MM-dd'T'HH:mm").alias("timestamp"), 
            F.col("item.temperature_2m").cast("float").alias("temperature"),
            F.col("item.wind_speed_10m").cast("float").alias("windspeed"),
            F.col("item.wind_direction_10m").cast("float").alias("winddirection"),
            F.col("item.weather_code").cast("int").alias("weathercode")
        )

        # Ví dụ minh họa df_cleaned sẽ có cấu trúc như sau:
        # station_id | timestamp           | temperature | windspeed | winddirection | weathercode
        # -----------|---------------------|-------------|-----------|---------------|-------------
        # HaNoi      | 2024-05-20 00:00:00 | 25.0        | 5.0       | 90.0          | 0
        # HaNoi      | 2024-05-20 00:15:00 | 26.0        | 6.0       | 100.0         | 1
        # - Mỗi hàng tương ứng với một mốc thời gian cụ thể, với các giá trị đã được ép kiểu chuẩn và sẵn sàng để ghi vào Silver Layer.


        # =============================================================================
        # Bước 4. Lọc dữ liệu: Chỉ giữ lại dữ liệu lịch sử (<= thời điểm hiện tại)
        # Đảm bảo dữ liệu nằm trong khoảng thời gian yêu cầu (quan trọng khi dùng wildcard ở Bước 1.1)
        st_str = processing_start_dt.strftime("%Y-%m-%d")
        en_str = processing_end_dt.strftime("%Y-%m-%d")
        
        df_final = df_cleaned.filter(
            F.to_date("timestamp").between(st_str, en_str)
        ).filter(
            # Loại bỏ các dữ liệu dự báo (Forecast) có trong API response
            F.col("timestamp") <= F.current_timestamp()
        )

        # Tối ưu hóa ghi: Gom nhóm theo ngày và sắp xếp. 
        # Việc này cực kỳ quan trọng để tránh OOM khi xử lý nhiều năm dữ liệu.
        df_final = df_final.repartition(6, F.date_trunc("day", F.col("timestamp"))) \
                           .sortWithinPartitions("timestamp")
        # sortWithinPartitions("timestamp"): Khi dữ liệu được sắp xếp theo cột partition (timestamp), 
        # - Spark sẽ ghi xong toàn bộ dữ liệu của ngày 01/01/2020, đóng file lại, rồi mới mở file cho ngày 02/01/2020. 
        # - Điều này giải phóng bộ nhớ buffer ngay lập tức, triệt tiêu lỗi OutOfMemory.

        # =============================================================================
        # Bước 5. GHI DỮ LIỆU/MERGE INTO VÀO ICEBERG (Tính lũy đẳng - Idempotency)
        # Khởi tạo Database và Table nếu chưa có
        spark.sql("CREATE DATABASE IF NOT EXISTS weather_db")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS weather_db.weather_silver (
                station_id STRING,
                timestamp TIMESTAMP,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT
            ) USING iceberg
            PARTITIONED BY (days(timestamp))
        """)
        # giải thích chi tiết 'USING iceberg':
        # - Khi bạn chỉ định 'USING iceberg' trong câu lệnh CREATE TABLE, bạn đang nói với Spark rằng bạn muốn sử dụng Iceberg làm engine lưu trữ cho bảng này.
        # - Điều này có nghĩa là Spark sẽ sử dụng các API và tính năng của Iceberg để quản lý dữ liệu, bao gồm việc lưu trữ dữ liệu trên S3, 
        #       quản lý metadata, hỗ trợ tính năng ACID, và đặc biệt là khả năng thực hiện MERGE INTO một cách hiệu quả.
        # - Nếu bạn không chỉ định 'USING iceberg', Spark sẽ sử dụng engine lưu trữ mặc định (thường là Parquet hoặc ORC) 
        #       và sẽ không có khả năng tận dụng các tính năng nâng cao của Iceberg, đặc biệt là MERGE INTO.


        df_final.createOrReplaceTempView("staged_weather")
        # Vì sao lại cần tạo một View tạm thời?
        # - MERGE INTO trong Spark SQL hoạt động trên các bảng hoặc View.
        # - Bằng cách tạo một View tạm thời từ DataFrame đã được xử lý, 
        #       chúng ta có thể sử dụng cú pháp SQL để thực hiện MERGE INTO một cách trực quan
        # - Nếu không tạo View tạm thời, chúng ta sẽ phải sử dụng API DataFrame để thực hiện upsert, 
        #       điều này có thể phức tạp hơn và không tận dụng được tối đa các tính năng của Iceberg.

        # ============ MERGE INTO: Bảo đảm tính lũy đẳng (Idempotency)
        # Nếu Station + Timestamp đã tồn tại -> Cập nhật giá trị mới nhất (Tránh duplicate)
        # Nếu chưa có -> Chèn mới.
        spark.sql("""
            MERGE INTO weather_db.weather_silver t
            USING staged_weather s
            ON t.station_id = s.station_id AND t.timestamp = s.timestamp
            WHEN MATCHED THEN
                UPDATE SET
                    t.temperature = s.temperature,
                    t.windspeed = s.windspeed,  
                    t.winddirection = s.winddirection,
                    t.weathercode = s.weathercode
            WHEN NOT MATCHED THEN
                INSERT *
        """)

        logger.info("Ghi dữ liệu vào Silver Layer (Iceberg) thành công.")

        # Quy trình ghi xuống MinIO:
        # 1. Spark sẽ ghi dữ liệu vào thư mục tạm thời trên S3 (MinIO) theo cấu trúc partitioning đã định nghĩa trong CREATE TABLE.
        # 2. Sau khi ghi xong, Spark sẽ tự động di chuyển dữ liệu từ thư mục tạm thời này 
        #       vào đúng vị trí trong bucket "silver" theo cấu trúc partitioning (days=YYYY-MM-DD).

        # Ví dụ minh họa về cấu trúc dữ liệu sau khi ghi vào Silver Layer:
        # silver/
        # └── weather_db/
        #     └── weather_silver/
        #         ├── days=2024-05-20/
        #         │   ├── part-00000-xxxx.parquet
        #         │   ├── part-00001-xxxx.parquet
        #         │   └── ...
        #         ├── days=2024-05-21/
        #         │   ├── part-00000-xxxx.parquet
        #         │   ├── part-00001-xxxx.parquet
        #         │   └── ...
        #         └── days=2024-05-22/
        #             ├── part-00000-xxxx.parquet
        #             ├── part-00001-xxxx.parquet
        #             └── ...
        # - Mỗi thư mục days=YYYY-MM-DD chứa các file Parquet được Spark ghi ra, 
        #       và Iceberg sẽ quản lý metadata để đảm bảo dữ liệu được tổ chức hiệu quả và hỗ trợ truy vấn nhanh chóng.
        #       đồng thời hỗ trợ các tính năng nâng cao như ACID và time travel.

        # Vì sao lại dùng Spark hay Iceberg lại ghi dưới định dạng Parquet:
        # - Parquet là định dạng lưu trữ mặc định của cả Apache Spark và Apache Iceberg. 
        # - Nếu bạn không chỉ định gì thêm, hệ thống sẽ tự động chọn Parquet. Bạn không bắt buộc phải khai báo nó.
    

    except Exception as e:
        logger.error(f"Lỗi thực thi Spark Job: {str(e)}")
        raise e
    finally:
        # Luôn đóng session để giải phóng RAM cho các dịch vụ khác
        spark.stop()

if __name__ == "__main__":
    # Argparse cho phép script Spark nhận các tham số ngày từ Airflow DAG.
    parser = argparse.ArgumentParser(description="Spark Silver Layer Transformation for Weather Data")
    
    # --start-date và --end-date: Xác định phạm vi dữ liệu lịch sử cần xử lý.
    # Airflow sẽ truyền các giá trị này khi kích hoạt DAG.
    parser.add_argument("--start-date", help="Ngày bắt đầu (YYYY-MM-DD) để xử lý dữ liệu từ Bronze.")
    parser.add_argument("--end-date", help="Ngày kết thúc (YYYY-MM-DD) để xử lý dữ liệu từ Bronze.")
    
    args = parser.parse_args()

    run_weather_silver_transformation(start_date=args.start_date, end_date=args.end_date)