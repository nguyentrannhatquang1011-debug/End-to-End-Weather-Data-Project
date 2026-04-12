import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import argparse

import boto3
from botocore.client import Config
import requests
from requests.adapters import HTTPAdapter
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

# Tải biến môi trường
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("BatchExtractor")

class BronzeUploader:
    """Lớp hỗ trợ trích xuất dữ liệu từ API và tải lên tầng Bronze (MinIO)."""

    def __init__(self) -> None:
        """
        Khởi tạo kết nối S3 tới MinIO và thiết lập Session tối ưu cho Network I/O.
        
        Workflow:
        1. Khởi tạo S3 Client tương thích chuẩn S3v4 của MinIO.
        2. Thiết lập requests.Session() kèm HTTPAdapter (Connection Pooling) 
           để tái sử dụng socket, giảm overhead khi thực hiện nhiều request.
        """
        # Kết nối tới Object Storage (MinIO)
        # Sử dụng boto3 với cấu hình đặc biệt để tương thích với MinIO (S3v4, endpoint, credentials).
        self.s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
            config=Config(signature_version='s3v4'),
            region_name='us-east-1' # Tham số bắt buộc của boto3 nhưng MinIO không quá khắt khe
        )
        self.bucket_name = "bronze"

        # Tối ưu hóa Connection Pooling
        self.http_session = requests.Session()
        adapter = HTTPAdapter(pool_connections=5, pool_maxsize=10)
        self.http_session.mount("https://", adapter)
        self.http_session.mount("http://", adapter)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    def fetch_api_data(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Thực hiện gọi API trích xuất dữ liệu với cơ chế Retry thông minh.
        
        Cơ chế:
        - Sử dụng Exponential Backoff để tránh làm quá tải API Server khi gặp lỗi 429 hoặc 5xx.
        - Sử dụng Session đã khởi tạo ở __init__ để tận dụng Connection Pooling.

        Args:
            url: Endpoint của API.
            params: Tham số truy vấn.
            
        Returns:
            Dữ liệu JSON trả về từ API dưới dạng Dictionary.
        """
        try:
            # Timeout được đặt là 20 giây để tránh việc script bị treo vô hạn
            response = self.http_session.get(url, params=params, timeout=20)
            # Kiểm tra mã trạng thái HTTP, nếu gặp lỗi (4xx, 5xx) sẽ ném ra ngoại lệ RequestException
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi Network hoặc API khi trích xuất dữ liệu: {e}")
            # Bắt buộc phải raise lại lỗi để decorator @retry của tenacity có thể nhận biết và thực hiện thử lại
            raise e

    def upload_to_bronze(self, data: Dict[str, Any], folder: str, reference_date: Optional[datetime] = None) -> None:
        """
        Đẩy dữ liệu JSON lên MinIO theo cấu trúc Partitioning.
        Cấu trúc: bronze/{folder}/year=YYYY/month=MM/day=DD/{timestamp}.json

        Cơ chế:
        - Giúp Spark chỉ quét các thư mục cần thiết (Partition Pruning), 
        - 'reference_date' cực kỳ quan trọng cho Initial Load để đảm bảo 
           dữ liệu năm 2014 vào đúng folder year=2014.

        Args:
            data: Dữ liệu JSON cần lưu.
            folder: Tên thư mục cha (weather hoặc aqi).
            reference_date: Ngày gắn liền với dữ liệu (dùng cho Backfill).
        """
        # nếu reference_date là None, sẽ dùng datetime.now() để tạo partition theo ngày hiện tại
        target_dt = reference_date or datetime.now()

        # Tạo path theo chuẩn Partitioning để Spark đọc hiệu quả hơn
        partition_path = (
            f"{folder}/year={target_dt.year}/month={target_dt.month:02d}/"
            f"day={target_dt.day:02d}" # ':02d' đảm bảo tháng và ngày luôn có 2 chữ số (vd: month=03, day=07) để dễ dàng sắp xếp và đọc bằng Spark
        )

        # File name sử dụng timestamp để tránh ghi đè nếu chạy nhiều lần trong ngày
        file_name = f"{int(target_dt.timestamp())}.json"

        s3_key = f"{partition_path}/{file_name}"
        # Ví dụ: bronze/weather/year=2024/month=03/day=27/1700000000.json
        # self.bucket_name= bronze, partition_path và file_name được tạo ở trên

        try:
            # =======Ghi dữ liệu thô (Raw) vào Bronze layer=============
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key, # Đường dẫn đầy đủ trong bucket, bao gồm partition và file name
                Body=json.dumps(data), # Chuyển Dict thành JSON String để lưu vào file
                ContentType='application/json' # Đặt Content-Type để dễ dàng xử lý sau này, mặc dù MinIO không bắt buộc nhưng là best practice khi làm việc với Object Storage
            )
            logger.info(f"Đã lưu dữ liệu thô vào: s3://{self.bucket_name}/{s3_key}")
        except Exception as e:
            logger.error(f"Lỗi khi upload lên MinIO: {e}")
            raise e

    def close(self) -> None:
        """Giải phóng tài nguyên Connection Pool."""
        self.http_session.close()

def extract_weather(start_date: Optional[str] = None, end_date: Optional[str] = None) -> None:
    """
    Workflow trích xuất dữ liệu thời tiết:
    1. Khởi tạo Uploader.
    2. Gọi API Open-Meteo lấy thông tin thời tiết Hà Nội.
    3. Đẩy kết quả thô vào bucket bronze/weather.

    Trích xuất dữ liệu thời tiết (Hỗ trợ cả Daily Batch và Initial Load 10 năm).

    Workflow cho Initial Load:
    - Nếu có start_date và end_date, sử dụng Archive API của Open-Meteo.
    - API này cho phép lấy dữ liệu từ năm 1940 đến nay.
    
    Lưu ý: Dữ liệu ở đây là RAW (chưa qua xử lý) để đảm bảo tính bất biến 
    của tầng Bronze theo kiến trúc Medallion.
    """
    uploader = BronzeUploader()
    
    # 1. Tính toán khoảng cách thời gian (nếu có tham số ngày được truyền vào)
    diff_days = 0
    if start_date and end_date:
        d1 = datetime.strptime(start_date, "%Y-%m-%d")
        d2 = datetime.strptime(end_date, "%Y-%m-%d")
        diff_days = (d2 - d1).days

    # 2. LUỒNG IF: Dành cho Initial Load (Dữ liệu lịch sử dài hạn trên 30 ngày)
    if start_date and end_date and diff_days > 30:
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": 21.0285,
            "longitude": 105.8542,
            "start_date": start_date,
            "end_date": end_date,
            # Với Initial Load (> 6 tháng), chỉ lấy dữ liệu theo giờ (hourly) để tối ưu hóa 
            # dung lượng lưu trữ Bronze và tránh tràn RAM khi Spark xử lý khối lượng lớn file.
            "hourly": "temperature_2m,wind_speed_10m,wind_direction_10m,weather_code",
            "timezone": "Asia/Bangkok"
        }
        logger.info(f"Đang thực hiện Initial Load (> 30 ngày) từ {start_date} đến {end_date}...")

    # 3. LUỒNG ELSE: Dành cho Backfill (Dưới 30 ngày) hoặc Incremental hàng ngày
    else:
        if start_date and end_date: # Nếu có tham số ngày nhưng dưới 30 ngày, ta hiểu là Backfill ngắn hạn
            # Trường hợp Backfill dữ liệu lịch sử trong ngắn hạn
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": 21.0285,
                "longitude": 105.8542,
                "start_date": start_date,
                "end_date": end_date,
                # Backfill ngắn hạn cho phép lấy cả hourly và minutely_15 để có Dashboard chi tiết
                "hourly": "temperature_2m,wind_speed_10m,wind_direction_10m,weather_code",
                "minutely_15": "temperature_2m,wind_speed_10m,wind_direction_10m,weather_code",
                "timezone": "Asia/Bangkok"
            }
            logger.info(f"Đang thực hiện Backfill (< 30 ngày) từ {start_date} đến {end_date}...")
        else:
            # Trường hợp không có tham số ngày, ta hiểu là chạy hàng ngày (Incremental), chỉ lấy dữ liệu của ngày hôm nay để tối ưu payload và RAM.
            today_str = datetime.now().strftime("%Y-%m-%d")
            
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": 21.0285,
                "longitude": 105.8542,
                "start_date": today_str, # Ép API chỉ lấy dữ liệu từ đầu ngày hôm nay
                "end_date": today_str,   # Đến cuối ngày hôm nay
                "current_weather": "true", # Snapshot cho luồng live
                "hourly": "temperature_2m,wind_speed_10m,wind_direction_10m,weather_code",
                "minutely_15": "temperature_2m,wind_speed_10m,wind_direction_10m,weather_code",
                "timezone": "Asia/Bangkok"
            }
            logger.info(f"Đang trích xuất Incremental cho ngày hôm nay: {today_str}")
    
    data = uploader.fetch_api_data(url, params)
    if data:
        # Đối với Initial Load, ta lấy ngày bắt đầu làm reference để phân vùng
        ref_dt = datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
        uploader.upload_to_bronze(data, "weather", reference_date=ref_dt)
    else:
        logger.error("Không thể lấy dữ liệu Weather từ Open-Meteo.")
    
    uploader.close()

def extract_aqi(start_date: Optional[str] = None, end_date: Optional[str] = None) -> None:
    """
    Workflow trích xuất dữ liệu chất lượng không khí (AQI):
    Sử dụng Open-Meteo Air Quality API để thay thế cho AQICN API trong luồng Batch 
    nhằm hỗ trợ lấy dữ liệu lịch sử (Historical Data).
    
    Workflow:
    1. Khởi tạo Uploader.
    2. Tính toán khoảng thời gian (diff_days) để quyết định chiến lược lấy dữ liệu.
    3. Gọi API Open-Meteo (Air Quality) lấy chỉ số PM2.5, PM10.
    4. Lưu trữ dữ liệu RAW vào Bronze Layer (MinIO).

    Args:
        start_date: Ngày bắt đầu (YYYY-MM-DD).
        end_date: Ngày kết thúc (YYYY-MM-DD).
    """
    uploader = BronzeUploader()

    # 1. Tính toán khoảng cách thời gian
    diff_days = 0
    if start_date and end_date:
        d1 = datetime.strptime(start_date, "%Y-%m-%d")
        d2 = datetime.strptime(end_date, "%Y-%m-%d")
        diff_days = (d2 - d1).days

    # 2. XÁC ĐỊNH ENDPOINT VÀ PARAMS (Tương tự extract_weather)
    # Open-Meteo Air Quality API sử dụng chung 1 endpoint cho cả forecast và archive
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    
    # Các tham số chung cho cả Initial Load và Incremental
    base_params = {
        "latitude": 21.0285,
        "longitude": 105.8542,
        "timezone": "Asia/Bangkok"
    }

    # LUỒNG IF: Dành cho Initial Load AQI (> 30 ngày)
    if start_date and end_date and diff_days > 30:
        params = {
            **base_params, # Kế thừa các tham số chung đã định nghĩa ở base_params
            "start_date": start_date,
            "end_date": end_date,
            # Lấy PM2.5 và PM10 theo giờ để tính US-AQI ở tầng Silver
            "hourly": "pm2_5,pm10",
        }
        logger.info(f"Đang thực hiện Initial Load AQI (> 30 ngày) từ {start_date} đến {end_date}...")

    # LUỒNG ELSE: Dành cho Backfill AQI ngắn hạn hoặc Incremental hàng ngày
    else:
        if start_date and end_date:
            # Backfill ngắn hạn
            params = {
                **base_params,
                "start_date": start_date,
                "end_date": end_date,
                "hourly": "pm2_5,pm10",
            }
            logger.info(f"Đang thực hiện Backfill AQI (< 30 ngày) từ {start_date} đến {end_date}...")
        else:
            # Incremental hàng ngày (Daily Batch)
            # Giới hạn chỉ lấy dữ liệu ngày hôm nay để tối ưu payload
            today_str = datetime.now().strftime("%Y-%m-%d")
            params = {
                **base_params,
                "start_date": today_str,
                "end_date": today_str,
                "hourly": "pm2_5,pm10",
                # Snapshot hiện tại để đối chiếu với luồng live
                "current": "pm2_5,pm10"
            }
            logger.info(f"Đang trích xuất Incremental AQI cho ngày hôm nay: {today_str}")

    # 3. THỰC THI VÀ UPLOAD
    data = uploader.fetch_api_data(url, params)
    
    if data:
        # Xác định reference_date để phân vùng dữ liệu chính xác trên MinIO
        ref_dt = datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
        uploader.upload_to_bronze(data, "aqi", reference_date=ref_dt)
    else:
        logger.error("Không thể lấy dữ liệu AQI từ Open-Meteo.")

    uploader.close()

if __name__ == "__main__":
    # =============================================================================
    # CẤU HÌNH ARGPARSE - QUẢN LÝ THAM SỐ DÒNG LỆNH (CLI)
    # =============================================================================
    # Tại sao cần argparse trong Data Engineering?
    # 1. Tính linh hoạt: Cho phép cùng một script chạy ở nhiều chế độ (weather, aqi).
    # 2. Tích hợp Orchestrator (Airflow): Airflow sẽ gọi script này và truyền giá trị vào 
    #    các tham số (flags) để điều khiển luồng dữ liệu.

    parser = argparse.ArgumentParser(description="Batch Extractor for Data Lakehouse")

    # --type: Xác định loại dữ liệu cần xử lý.
    # Giúp chúng ta có thể tách nhỏ Task trên Airflow (Task 1: Weather, Task 2: AQI) 
    # để dễ dàng theo dõi và xử lý lỗi riêng biệt (Fault Tolerance).
    parser.add_argument("--type", choices=["weather", "aqi", "all"], default="all", help="Loại dữ liệu cần lấy")

    # --start-date và --end-date: Quyết định phạm vi dữ liệu lịch sử.
    # Workflow trên Airflow:
    # - Nếu chạy Daily: Airflow truyền --start-date và --end-date là cùng một ngày (vd: 2024-05-20).
    # - Nếu chạy Initial Load: Admin có thể kích hoạt Airflow Backfill để truyền phạm vi 10 năm.
    parser.add_argument("--start-date", help="Ngày bắt đầu (YYYY-MM-DD) cho Initial Load")
    parser.add_argument("--end-date", help="Ngày kết thúc (YYYY-MM-DD) cho Initial Load")
    
    # parse_args() sẽ bóc tách các giá trị từ dòng lệnh vào object 'args'.
    # Ví dụ khi chạy: python batch_extractor.py --type weather --start-date 2014-01-01
    # Thì args.type = 'weather' và args.start_date = '2014-01-01'
    args = parser.parse_args()

    # =============================================================================
    # QUY TRÌNH THỰC THI (EXECUTION WORKFLOW) TRÊN AIRFLOW
    # 1. Airflow Scheduler kích hoạt Task thông qua BashOperator.
    # 2. Script được khởi tạo, argparse đọc tham số được truyền từ Airflow.
    # 3. Hệ thống kiểm tra điều kiện bên dưới để gọi hàm trích xuất tương ứng.
    # =============================================================================

    # Nếu có tham số ngày, script sẽ tự hiểu là Initial Load, nếu không sẽ chạy hàng ngày (Incremental)
    # Nếu --type là 'all', sẽ chạy cả hai loại dữ liệu. Nếu chỉ 'weather' hoặc 'aqi', sẽ chỉ chạy loại đó.
    if args.type in ["weather", "all"]:
        # Nếu có tham số ngày, script sẽ tự hiểu là Initial Load
        extract_weather(start_date=args.start_date, end_date=args.end_date)
    
    if args.type in ["aqi", "all"]:
        # Bây giờ extract_aqi cũng nhận start_date và end_date để hỗ trợ Initial Load
        extract_aqi(start_date=args.start_date, end_date=args.end_date)
