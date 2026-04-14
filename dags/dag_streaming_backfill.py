from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.param import Param
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

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
    # Khai báo Params để tạo Form nhập liệu trên giao diện Airflow
    params={
        "start_date": Param(
            default=datetime.now().strftime('%Y-%m-%d'),
            type="string",
            format="date",
            description="Ngày bắt đầu backfill (YYYY-MM-DD)"
        ),
        "end_date": Param(
            default=datetime.now().strftime('%Y-%m-%d'),
            type="string",
            format="date",
            description="Ngày kết thúc backfill (YYYY-MM-DD)"
        ),
    },
    render_template_as_native_obj=True, # Cho phép sử dụng Jinja templates trong các tham số, đặc biệt là để truyền tham số vào bash_command của BashOperator.
    tags=['streaming', 'backfill', 'influxdb', 'duckdb'],
) as dag:
    
    # SCRIPTS_PATH trong container
    SCRIPTS_PATH = "/opt/airflow/scripts"

    # Truy cập giá trị từ Params (Form nhập liệu)
    # Lưu ý: Bấm vào nút Play lớn trên UI sẽ hiện ra bảng hỏi ngay lập tức.
    START_DATE = "{{ params.start_date }}"
    END_DATE = "{{ params.end_date }}"
    

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
