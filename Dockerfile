# ==============================================================================
# DOCKERFILE: Cấu hình môi trường chạy cho Airflow & PySpark
# ==============================================================================

# 1. Sử dụng bản Airflow chính thức (Python 3.10) làm nền tảng (Base Image)
FROM apache/airflow:2.8.1-python3.10

# 2. Chuyển sang quyền 'root' để thực hiện các thao tác quản trị hệ thống
# Mặc định image của Airflow chạy với user 'airflow', không có quyền cài đặt phần mềm.
USER root

# 3. Cập nhật danh sách gói và cài đặt Java Runtime (JRE)
# Java là thành phần bắt buộc nếu bạn muốn chạy PySpark bên trong Airflow Tasks.
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
         procps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* 
# (Dòng cuối '-rm' giúp xóa các file tạm sau khi cài đặt để làm nhẹ dung lượng Image)

# 4. Chuyển ngược lại về user 'airflow' để đảm bảo tính bảo mật
# Không nên chạy ứng dụng bằng quyền root nếu không thực sự cần thiết.
USER airflow

# 5. Thiết lập biến môi trường JAVA_HOME
# Giúp các thư viện như PySpark biết chính xác nơi Java được cài đặt để khởi động.
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Cài đặt các thư viện Python từ requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6. Tạo thư mục cache cho Spark/Ivy và phân quyền
# Điều này đảm bảo khi chạy container với user UID:0, Spark có quyền ghi vào cache
USER root
# Tạo thư mục cache và scratch space cho Spark bên trong /opt/airflow.
# Tránh sử dụng /tmp vì sticky bit có thể gây lỗi AccessDenied khi mount volume từ Windows.
# Cấp quyền 777 để đảm bảo User airflow (50000) có toàn quyền thao tác trên dữ liệu shuffle.
RUN mkdir -p /opt/airflow/spark_temp /opt/airflow/spark_ivy /opt/airflow/duckdb_extensions /home/airflow/.cache \
    && chown -R airflow:root /opt/airflow/spark_temp /opt/airflow/spark_ivy /opt/airflow/duckdb_extensions /home/airflow/.cache \
    && chmod -R 775 /home/airflow/.cache \
    && chmod 777 /opt/airflow/spark_temp \
    && chmod 777 /opt/airflow/spark_ivy \
    && chmod 777 /opt/airflow/duckdb_extensions
# why 775? - Cho phép user 'airflow' và nhóm 'root' có quyền đọc/ghi/xóa, trong khi vẫn cho phép các user khác đọc/execute (không có quyền ghi).
# why not 777? - Tránh cấp quyền ghi cho tất cả mọi người, điều này có thể dẫn đến rủi ro bảo mật nếu container bị tấn công.    

USER airflow

# Thực hiện tải sẵn các thư viện vào đúng thư mục cấu hình để tăng tốc độ khởi động Job.
RUN python3 -c "from pyspark.sql import SparkSession; \
    SparkSession.builder.config('spark.jars.packages', \
    'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262') \
    .config('spark.jars.ivy', '/opt/airflow/spark_ivy') \
    .getOrCreate().stop()"
# org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 - là gói thư viện chính để tích hợp Iceberg với Spark 3.5
# org.apache.hadoop:hadoop-aws:3.3.4 - là gói thư viện để Spark có thể đọc/ghi dữ liệu trực tiếp từ S3 (MinIO) thông qua giao thức s3a.
# com.amazonaws:aws-java-sdk-bundle:1.12.262 - là gói thư viện AWS SDK cần thiết để hỗ trợ các thao tác với S3 (MinIO) như xác thực, quản lý bucket, v.v.

# Thực hiện tải sẵn các extension của DuckDB để tránh lỗi IO Error khi chạy script backfill trong container
RUN python3 -c "import duckdb; conn = duckdb.connect(); \
    conn.execute(\"SET extension_directory='/opt/airflow/duckdb_extensions';\"); \
    conn.execute('INSTALL httpfs; INSTALL iceberg;')"
# Đặt thư mục làm việc mặc định trong container là /opt/airflow, nơi Airflow sẽ tìm kiếm DAGs và các file cấu hình khác.
WORKDIR /opt/airflow 