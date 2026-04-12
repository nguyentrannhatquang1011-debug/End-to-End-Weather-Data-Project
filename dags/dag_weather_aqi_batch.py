from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# =============================================================================
# CẤU HÌNH THÔNG SỐ DAG
# =============================================================================
# 'ds' là template variable của Airflow đại diện cho ngày thực thi (YYYY-MM-DD)
# Chúng ta sẽ truyền nó vào --start-date và --end-date của các script.

default_args = {
    'owner': 'quang_nguyen',
    'depends_on_past': False, # Không phụ thuộc vào lượt chạy trước đó, giúp dễ dàng chạy lại nếu cần.
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hanoi_weather_aqi_batch_pipeline',
    default_args=default_args,
    description='Pipeline thu thập và xử lý dữ liệu Weather/AQI Hà Nội (Medallion Architecture)',
    # Lịch chạy: 3:00 PM (15:00) hàng ngày
    schedule_interval='0 15 * * *', # Cron expression để chạy duy nhất vào lúc 15:00:00
    start_date=datetime(2026, 4, 1), # Ngày 2026-04-01 (YYYY-MM-DD)
    catchup=True,
    tags=['weather', 'aqi', 'iceberg', 'gold_aggregation'], # Thêm tag để dễ dàng lọc và quản lý trong Airflow UI
    max_active_runs=1, # Đảm bảo chỉ có 2 lượt chạy DAG hoạt động tại một thời điểm để tránh tràn RAM khi chạy Backfill
) as dag:
    # Giải thích chi tiết 'catchup=True':
    # - Khi 'catchup' được đặt thành True, Airflow sẽ tự động chạy tất cả các lượt chạy bị bỏ lỡ kể từ 'start_date' cho đến ngày hiện tại.
    # - Ví dụ: Nếu 'start_date' là 1/1/2024 và hôm nay là 20/5/2024, khi bạn kích hoạt DAG, Airflow sẽ tạo và chạy lượt chạy cho tất cả các ngày từ 1/1/2024 đến 20/5/2024.
    # - Điều này rất hữu ích để đảm bảo rằng tất cả dữ liệu trong quá khứ được xử lý, đặc biệt khi bạn mới triển khai DAG hoặc khi có sự cố khiến DAG không chạy trong một khoảng thời gian.


    # Khai báo đường dẫn gốc tới các script trong container
    SCRIPTS_PATH = "/opt/airflow/scripts"

    # =========================================================================
    # CỤM 1: EXTRACTION (BRONZE LAYER)
    # Trích xuất dữ liệu thô từ API và đẩy vào MinIO Bronze dưới dạng JSON
    # =========================================================================

    # Giải thích {{ ds }}:
    # - Đây là một template variable của Airflow, đại diện cho ngày thực thi của lượt chạy đó.
    # - Khi Airflow chạy task này, nó sẽ tự động thay thế {{ ds }} bằng ngày thực thi (ví dụ: '2024-05-20').
    # - Điều này giúp đảm bảo rằng script batch_extractor.py chỉ trích xuất dữ liệu của ngày hôm đó, tối ưu hóa I/O và RAM.
    extract_weather_task = BashOperator(
        task_id='extract_weather_bronze',
        bash_command=(
            f"python {SCRIPTS_PATH}/batch_extractor.py "
            "--type weather --start-date {{ ds }} --end-date {{ ds }}"
        )
    )


    extract_aqi_task = BashOperator(
        task_id='extract_aqi_bronze',
        bash_command=(
            f"python {SCRIPTS_PATH}/batch_extractor.py "
            "--type aqi --start-date {{ ds }} --end-date {{ ds }}"
        )
    )

    # =========================================================================
    # CỤM 2: TRANSFORMATION (SILVER LAYER)
    # Sử dụng PySpark để làm sạch, flatten dữ liệu và MERGE INTO Iceberg Table.
    # Lưu ý: Các task Spark chạy tuần tự để không làm tràn RAM 16GB.
    # =========================================================================

    ICEBERG_VERSION = "1.5.0"
    HADOOP_AWS_VERSION = "3.3.4"
    PACKAGES = f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION},org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}"
    
    # Giải thích '--master local[*]':
    # - master: Đây là tham số quan trọng để xác định chế độ chạy của Spark, chỉ đinh danh nơi Spark sẽ thực thi các tác vụ của mình.
    # - local: Nghĩa là Spark sẽ chạy toàn bộ các thành phần (Driver và Executor) bên trong duy nhất một tiến trình (JVM) tại máy local. 
    #       Nó không tìm kiếm các máy chủ khác trong mạng. Điều này cực kỳ hoàn hảo cho môi trường Docker hoặc máy cá nhân
    # - [6]: Đây là phần quan trọng nhất về hiệu năng. Dấu '* hoặc 6' đại diện cho số lượng luồng (threads) song song.
    #       Nếu bạn ghi local[6]: Spark sẽ sử dụng 6 luồng để xử lý dữ liệu song song.

    # Nếu bạn xóa --master local[*] khỏi lệnh spark-submit trong file dag_weather_aqi_batch.py và cũng không khai báo trong code Python:
    # - Spark sẽ tìm cấu hình mặc định trong file spark-defaults.conf (thường không có trong Docker).
    # - Nếu không thấy, Spark thường mặc định chạy ở chế độ local (không có dấu *). Khi đó, nó chỉ dùng 1 Core duy nhất.
    # - Hậu quả: Job của bạn sẽ chạy cực kỳ chậm vì không tận dụng được sức mạnh đa nhân của CPU 8 Cores.

    SPARK_SUBMIT_CMD = (
        f"spark-submit --master local[6] "
        f"--packages {PACKAGES} "
        f"--conf spark.jars.ivy=/home/airflow/.ivy2 "
        # Giới hạn RAM tối đa cho Spark Driver (trong Local mode, Driver bao gồm luôn Executor).
        # Việc đặt 4G giúp ngăn Spark "vắt kiệt" RAM của máy host 16GB.
        f"--driver-memory 4g "
        # Cấu hình bộ nhớ đệm (Memory Overhead) để tránh lỗi OOM ở tầng Off-heap (thường do PySpark/JVM giao tiếp).
        f"--conf spark.driver.memoryOverhead=512m "
        # Tối ưu hóa việc dọn dẹp bộ nhớ: Giảm tỷ lệ bộ nhớ dành cho Caching để ưu tiên cho Execution (xử lý Join/Shuffle).
        # Phù hợp với máy RAM thấp.
        f"--conf spark.memory.fraction=0.6 "
        f"--conf spark.memory.storageFraction=0.2 "
        # Ép Spark sử dụng Host Volume cho Scratch Space thông qua tham số dòng lệnh.
        # Giúp giải phóng dung lượng ổ đĩa ngay lập tức trên máy Windows sau khi task hoàn thành.
        # QUAN TRỌNG: Thư mục này phải khớp với đường dẫn đã mount trong docker-compose.yml 
        # và đã được phân quyền trong Dockerfile để tránh lỗi AccessDeniedException.
        f"--conf spark.local.dir=/opt/airflow/spark_temp "
    )

    silver_weather_task = BashOperator(
        task_id='transform_weather_silver',
        bash_command=(
            f"{SPARK_SUBMIT_CMD} {SCRIPTS_PATH}/spark_silver_weather.py "
            "--start-date {{ ds }} --end-date {{ ds }}"
        )
    )

    silver_aqi_task = BashOperator(
        task_id='transform_aqi_silver',
        bash_command=(
            f"{SPARK_SUBMIT_CMD} {SCRIPTS_PATH}/spark_silver_aqi.py "
            "--start-date {{ ds }} --end-date {{ ds }}"
        )
    )

    # =========================================================================
    # CỤM 3: PRE-AGGREGATION (GOLD LAYER)
    # Tổng hợp dữ liệu theo ngày (Daily Summary) để phục vụ Dashboard.
    # Sử dụng logic 'all' để xử lý cả 2 loại dữ liệu trong cùng một tiến trình Spark tuần tự.
    # =========================================================================

    gold_aggregation_task = BashOperator(
        task_id='aggregate_gold_summary',
        bash_command=(
            f"{SPARK_SUBMIT_CMD} {SCRIPTS_PATH}/spark_gold_summary.py "
            "--type all --start-date {{ ds }} --end-date {{ ds }}"
        )
    )

    # =========================================================================
    # THIẾT LẬP THỨ TỰ THỰC THI (DEPENDENCIES)
    # =========================================================================

    [extract_weather_task, extract_aqi_task] >> silver_weather_task >> silver_aqi_task >> gold_aggregation_task

    #============ Task Pipeline ===============
    # To use a list for the extraction tasks, you are essentially telling Airflow that these 2 tasks can run in parallel.
    # Visualization: In the Airflow Graph View, you will see the pipeline "fan out" 
    #       into 2 branches after the unzip task and then "fan back in" to the consolidate task.
    # "silver_weather_task" will wait for all 2 tasks in the list to succeed before it starts. If even one of them (like the TSV task) fails, 
    #       the silver_weather_task task will stay in a "Upstream Failed" state.

"""
GIẢI THÍCH CHI TIẾT LOGIC TRONG DAG:

1. Scheduling (Lịch chạy):
   Sử dụng Cron '30 9,16 * * *' để đáp ứng đúng yêu cầu chạy vào 9:30 sáng và 4:00 chiều. 
   Airflow sẽ kích hoạt DAG tại các thời điểm này.

2. Jinja Templating ({{ ds }}):
   Đây là yếu tố then chốt cho tính Idempotency. '{{ ds }}' sẽ được Airflow thay thế 
   bằng ngày thực thi của lượt chạy đó (ví dụ: '2024-05-20'). Điều này giúp script 
   Spark/Python chỉ xử lý đúng tập dữ liệu của ngày hôm đó, tối ưu hóa I/O và RAM.

3. Quản lý tài nguyên (RAM 16GB):
   - max_active_runs=1: Đảm bảo tại một thời điểm chỉ có 1 lượt chạy DAG được hoạt động. 
     Tránh trường hợp Backfill chạy song song nhiều ngày gây treo máy.
   - Tuần tự hóa Spark Tasks: Chúng ta không chạy 'silver_weather' và 'silver_aqi' 
     song song. Spark sẽ chiếm dụng CPU/RAM rất lớn, nên việc chạy tuần tự giúp 
     hệ thống ổn định hơn trên môi trường Docker Local.

4. Idempotency (Tính lũy đẳng):
   Vì chúng ta truyền --start-date và --end-date là cùng một giá trị {{ ds }}, 
   kết hợp với lệnh MERGE INTO trong các file Spark, bạn có thể chạy lại các Task 
   này bất kỳ lúc nào mà không sợ bị trùng lặp dữ liệu trong Data Lake.
"""