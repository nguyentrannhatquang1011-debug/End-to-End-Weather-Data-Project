from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# =============================================================================
# CẤU HÌNH THÔNG SỐ DAG - INITIAL LOAD (CHẠY THỦ CÔNG)
# =============================================================================
# DAG này được thiết kế để nạp dữ liệu một lần duy nhất từ năm 2020 đến 2026.
# Do dữ liệu nhẹ, chúng ta gom toàn bộ khoảng thời gian vào một lần thực thi duy nhất.

default_args = {
    'owner': 'quang_nguyen',
    'depends_on_past': False, # Không phụ thuộc vào quá khứ vì đây là luồng chạy độc lập
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hanoi_initial_load_2020_2026',
    default_args=default_args,
    description='Nạp dữ liệu lịch sử Weather/AQI từ 01/01/2020 đến 31/03/2026',
    # schedule=None: DAG sẽ không tự động chạy theo lịch, chỉ chạy khi nhấn Trigger trên UI.
    schedule=None,
    start_date=datetime(2020, 1, 1),
    # catchup=False: Không tự động tạo các lượt chạy bù (Backfill) theo từng ngày.
    catchup=False,
    tags=['initial_load', 'manual', 'batch'],
    max_active_runs=1,
) as dag:

    SCRIPTS_PATH = "/opt/airflow/scripts"
    
    # Định nghĩa khoảng thời gian cố định cho Initial Load
    START_DATE = "2020-01-01"
    END_DATE = "2026-03-31"

    # Cấu hình Spark Submit (Tối ưu RAM 16GB và Scratch Space ra Host Volume)
    ICEBERG_VERSION = "1.5.0"
    HADOOP_AWS_VERSION = "3.3.4"
    PACKAGES = f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION},org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}"

    SPARK_SUBMIT_CMD = (
        f"spark-submit --master local[6] "
        f"--packages {PACKAGES} "
        f"--conf spark.jars.ivy=/opt/airflow/spark_ivy "
        # Tăng RAM lên 6G cho Initial Load vì dải dữ liệu 2020-2026 rất lớn
        f"--driver-memory 6g "
        f"--conf spark.driver.memoryOverhead=1g "
        f"--conf spark.memory.fraction=0.6 "
        f"--conf spark.memory.storageFraction=0.2 "
        f"--conf spark.local.dir=/opt/airflow/spark_temp "
        # Giới hạn số lượng partition khi shuffle để tránh overhead trên máy 16GB
        f"--conf spark.sql.shuffle.partitions=6 "
    )

    # =========================================================================
    # TASK 1: TRÍCH XUẤT DỮ LIỆU THÔ (BRONZE)
    # =========================================================================
    # Script batch_extractor.py đã được thiết kế để xử lý một dải ngày dài 
    # thông qua Archive API của Open-Meteo.

    extract_weather_task = BashOperator(
        task_id='extract_weather_bronze_bulk',
        bash_command=(
            f"python {SCRIPTS_PATH}/batch_extractor.py "
            f"--type weather --start-date {START_DATE} --end-date {END_DATE}"
        )
    )

    extract_aqi_task = BashOperator(
        task_id='extract_aqi_bronze_bulk',
        bash_command=(
            f"python {SCRIPTS_PATH}/batch_extractor.py "
            f"--type aqi --start-date {START_DATE} --end-date {END_DATE}"
        )
    )

    # =========================================================================
    # TASK 2: CHUYỂN ĐỔI VÀ GHI VÀO LAKEHOUSE (SILVER)
    # =========================================================================
    # Spark sẽ quét toàn bộ các file JSON trong dải ngày và thực hiện MERGE INTO.

    silver_weather_task = BashOperator(
        task_id='transform_weather_silver_bulk',
        bash_command=(
            f"{SPARK_SUBMIT_CMD} {SCRIPTS_PATH}/spark_silver_weather.py "
            f"--start-date {START_DATE} --end-date {END_DATE}"
        )
    )

    silver_aqi_task = BashOperator(
        task_id='transform_aqi_silver_bulk',
        bash_command=(
            f"{SPARK_SUBMIT_CMD} {SCRIPTS_PATH}/spark_silver_aqi.py "
            f"--start-date {START_DATE} --end-date {END_DATE}"
        )
    )

    # =========================================================================
    # TASK 3: TỔNG HỢP DỮ LIỆU (GOLD)
    # =========================================================================

    gold_aggregation_task = BashOperator(
        task_id='aggregate_gold_summary_bulk',
        bash_command=(
            f"{SPARK_SUBMIT_CMD} {SCRIPTS_PATH}/spark_gold_summary.py "
            f"--type all --start-date {START_DATE} --end-date {END_DATE}"
        )
    )

    # =========================================================================
    # THỨ TỰ THỰC THI
    # =========================================================================
    # Chạy trích xuất trước, sau đó mới thực hiện Spark Transformation.
    extract_weather_task >> extract_aqi_task >> silver_weather_task >> silver_aqi_task >> gold_aggregation_task