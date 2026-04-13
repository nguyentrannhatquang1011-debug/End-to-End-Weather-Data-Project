from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'quang_nguyen',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hanoi_streaming_backfill_pipeline',
    default_args=default_args,
    description='Hồi phục dữ liệu Dashboard (InfluxDB) từ Lakehouse hoặc API',
    schedule_interval=None,  # Chạy thủ công khi phát hiện Dashboard bị trống
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['streaming', 'backfill', 'influxdb', 'duckdb'],
) as dag:
    
    # SCRIPTS_PATH trong container
    SCRIPTS_PATH = "/opt/airflow/scripts"

    # Sử dụng Jinja template để cho phép truyền tham số ngày từ UI (Run with config)
    # Mặc định nếu không truyền sẽ lấy ngày hôm nay.
    # Ví dụ config: {"start_date": "2024-03-01", "end_date": "2024-03-05"}

    # Giải thích 'dag_run.conf': 
    #  - Đây là cách Airflow cho phép truy cập vào các tham số được truyền khi Trigger DAG từ UI. 
    #  - Nó không phải là logical date (i.e. 'ds'), mà là tham số tùy chọn do người dùng cung cấp.

    # Người dùng cung cấp như thế nào?:
    #   + Khi Trigger DAG, Airflow UI sẽ hiển thị một form để nhập JSON config.
    #   + Người dùng có thể nhập: {"start_date": "2024-03-01", "end_date": "2024-03-05"} để chỉ định khoảng thời gian cần backfill.
    #   + Nếu người dùng không nhập gì, DAG sẽ sử dụng ngày hiện tại (ds) làm mặc định, nghĩa là sẽ backfill cho ngày hôm nay.

    # Tại sao lại dùng 'dag_run.conf' thay vì 'ds'?:
    #  - 'ds' là logical date của DAG run, thường được dùng cho các DAG có schedule. 
    #  - Nhưng vì đây là DAG chạy thủ công, chúng ta muốn linh hoạt hơn trong việc chỉ định ngày cần backfill
    #  - Nếu người dùng không cung cấp tham số, nó sẽ sử dụng ngày hiện tại (ds) làm mặc định.
    
    START_DATE = "{{ dag_run.conf['start_date'] if dag_run and dag_run.conf.get('start_date') else ds }}"
    END_DATE = "{{ dag_run.conf['end_date'] if dag_run and dag_run.conf.get('end_date') else ds }}"

    backfill_weather_live = BashOperator(
        task_id='backfill_weather_to_influx',
        bash_command=(
            f"python {SCRIPTS_PATH}/streaming_backfill.py "
            f"--type weather --start-date {START_DATE} --end-date {END_DATE}"
        )
    )

    backfill_aqi_live = BashOperator(
        task_id='backfill_aqi_to_influx',
        bash_command=(
            f"python {SCRIPTS_PATH}/streaming_backfill.py "
            f"--type aqi --start-date {START_DATE} --end-date {END_DATE}"
        )
    )

    # Hai tác vụ có thể chạy song song vì InfluxDB hỗ trợ ghi đa luồng tốt
    [backfill_weather_live, backfill_aqi_live]

"""
GIẢI THÍCH WORKFLOW:
1. Khi Dashboard (Streamlit) bị mất dữ liệu do Consumer bị sập hoặc lỗi mạng Kafka.
2. Người vận hành vào Airflow, Trigger DAG này kèm cấu hình ngày bị thiếu.
3. Script python sẽ thực hiện:
   - Quét Silver Layer trong MinIO bằng DuckDB (Tốc độ cực nhanh).
   - Nếu dữ liệu chưa có trong Lakehouse (do Batch job chưa tới giờ chạy), script tự động gọi API Open-Meteo.
   - Dữ liệu được đẩy thẳng vào InfluxDB, làm đầy Dashboard ngay lập tức.
4. KHÔNG có bản tin nào được đẩy vào Kafka, đảm bảo tính an toàn cho luồng Live.
"""