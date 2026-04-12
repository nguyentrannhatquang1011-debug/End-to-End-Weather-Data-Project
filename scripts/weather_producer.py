import json
import time
import logging
import os
from typing import Optional, Dict, Any

import requests
from requests.adapters import HTTPAdapter
from confluent_kafka import Producer
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

# Tải biến môi trường từ file .env
load_dotenv()

# =============================================================================
# CẤU HÌNH LOGGING
# Format bao gồm: Thời gian - Tên Logger - Mức độ lỗi - Thông điệp.
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("WeatherProducer")

# =============================================================================
# TỐI ƯU HÓA CONNECTION POOLING
# =============================================================================
http_session = requests.Session()
# Cấu hình Adapter để kiểm soát pool_connections (số host) và pool_maxsize (số kết nối/host)
# Vì script này đơn luồng và chỉ gọi 1 host, ta đặt các giá trị nhỏ để tiết kiệm RAM.
adapter = HTTPAdapter(pool_connections=2, pool_maxsize=10)
http_session.mount("https://", adapter) # Đảm bảo tất cả request HTTPS sử dụng adapter này
http_session.mount("http://", adapter) # Đảm bảo tất cả request HTTP sử dụng adapter này

def delivery_report(err: Optional[Exception], msg: Any) -> None:
    """
    Hàm callback báo cáo kết quả gửi tin nhắn tới Kafka.
    
    Workflow:
    1. Hàm này được gọi tự động sau mỗi lệnh producer.produce() hoặc producer.flush().
    2. Nếu 'err' không rỗng: Kafka Broker xác nhận tin nhắn chưa được ghi (vd: mất kết nối).
    3. Nếu 'err' rỗng: Tin nhắn đã nằm an toàn trong Topic tại một Partition cụ thể.

    Args:
        err: Lỗi nếu tin nhắn không thể gửi đi (None nếu thành công).
        msg: Đối tượng tin nhắn chứa thông tin về topic và partition.
    """
    # Kiểm tra trạng thái gửi tin nhắn
    if err is not None:
        logger.error(f"Gửi tin nhắn thất bại: {err}")
    else:
        logger.info(f"Gửi thành công tới topic {msg.topic()} [Partition: {msg.partition()}]")

# =============================================================================
# HÀM LẤY DỮ LIỆU THỜI TIẾT
# =============================================================================
# Hàm này có cơ chế retry từ Tenacity để đảm bảo tính ổn định của luồng dữ liệu.
# Cơ chế retry của Tenacity: sẽ được áp dụng tự động khi hàm get_weather_data() ném ra một ngoại lệ (vd: HTTPError, Timeout).
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True
)
def get_weather_data(session: requests.Session) -> Dict[str, Any]:
    """
    Trích xuất dữ liệu thời tiết hiện tại từ Open-Meteo API.
    
    Cơ chế Retry (Tenacity):
    - Stop: Thử tối đa 3 lần.
    - Wait Exponential: Công thức: multiplier * (2^n). 
      Với multiplier=1, min=2: 
      Lần 1 đợi 2s (1*2^1), Lần 2 đợi 4s (1*2^2).
      Điều này giúp giảm tải cho server API khi họ đang gặp sự cố (Rate Limit).
    - Reraise: Ném lỗi cuối cùng ra ngoài sau khi đã hết lượt retry.

    Args:
        session: Đối tượng requests.Session được tái sử dụng để tối ưu Network.

    Returns:
        Dict[str, Any]: Dữ liệu JSON từ API trả về.

    Raises:
        requests.exceptions.HTTPError: Nếu API trả về mã lỗi HTTP.
    """
    # Tọa độ Hà Nội: 21.0285, 105.8542
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 21.0285,
        "longitude": 105.8542,
        "current_weather": "true", # Chỉ lấy dữ liệu thời tiết hiện tại để giảm tải và phù hợp với mục tiêu Live-feel
        "timezone": "Asia/Bangkok"
    }

    try:
        # Thực hiện request thông qua session chung
        # Giảm timeout xuống 15s để retry nhanh hơn nếu mạng chập chờn
        response = session.get(url, params=params, timeout=15)
        # Kiểm tra mã trạng thái HTTP (200 OK), nếu lỗi (4xx, 5xx) sẽ ném ngoại lệ.
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Lỗi khi gọi Open-Meteo API: {e}")
        raise e # Bắt buộc phải raise lại lỗi để Tenacity có thể nhận biết và thực hiện retry
    
    return response.json()

def run_producer() -> None:
    """
    Khởi tạo Kafka Producer và thực hiện luồng trích xuất - gửi dữ liệu vô hạn.
    
    Đây là luồng thực thi chính của Script Streaming Producer.
    """
    # Cấu hình lấy từ .env để đảm bảo tính bảo mật và linh hoạt khi triển khai.
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = "weather_live"
    
    # Tần suất lấy dữ liệu: 200 giây (3 phút 20 giây) để cân bằng giữa Live-feel và Load hệ thống
    POLL_INTERVAL = 200 
    
    # Cấu hình Kafka Producer
    producer_config = {
        'bootstrap.servers': kafka_servers,
        'client.id': 'weather-producer', # Định danh để theo dõi trên Kafka UI
        'acks': 'all', # Yêu cầu tất cả bản sao (replicas) xác nhận để đảm bảo không mất dữ liệu
        'compression.type': 'snappy' # Nén dữ liệu để tiết kiệm băng thông và Disk Space
    }
    
    try:
        producer = Producer(producer_config)
        logger.info(f"Đã khởi tạo Kafka Producer kết nối tới {kafka_servers}")

        # Biến lưu trữ mốc thời gian cuối cùng được ghi nhận từ API
        # Mục tiêu: Tránh đẩy dữ liệu trùng lặp khi API chưa cập nhật giá trị mới
        last_observed_time = None

        # Vòng lặp vô hạn đặc trưng của các hệ thống Streaming
        while True:
            try:
                # Bước 1: Gọi API lấy dữ liệu thời tiết (đã có cơ chế retry bảo vệ)
                weather_payload = get_weather_data(http_session)
                
                # Kiểm tra tính mới của dữ liệu dựa trên field 'time' của Open-Meteo
                current_time = weather_payload.get('current_weather', {}).get('time')
                
                # Skip Logic: Nếu dữ liệu lấy về có timestamp trùng khớp với lần request trước, 
                #   lệnh continue sẽ bỏ qua toàn bộ các bước tốn tài nguyên phía sau như json.dumps, encode, produce() và đặc biệt là flush().
                if current_time == last_observed_time:
                    logger.info(f"Dữ liệu thời tiết tại mốc {current_time} đã tồn tại. Bỏ qua chu kỳ này.")
                    time.sleep(POLL_INTERVAL) # Nghỉ trước khi kiểm tra lại
                    continue
                
                # Cập nhật trạng thái thời gian mới nhất
                last_observed_time = current_time

                # Bước 2: Chuẩn bị dữ liệu cho Kafka
                # - json.dumps: Chuyển Dict thành chuỗi JSON String
                # - encode('utf-8'): Chuyển chuỗi thành mảng Byte (Kafka chỉ nhận Byte)
                message_str = json.dumps(weather_payload)
                message_bytes = message_str.encode('utf-8')
                
                # Bước 3: Đưa tin nhắn vào hàng đợi gửi của Producer (Asynchronous)
                producer.produce(
                    topic=topic,
                    value=message_bytes,
                    callback=delivery_report # Đăng ký hàm kiểm tra kết quả gửi
                )
                
                # Bước 4: Flush - Ép gửi tin nhắn đi ngay lập tức.
                # Do script này chạy theo chu kỳ 200s/lần, chúng ta cần flush để đảm bảo 
                # dữ liệu xuất hiện trên Dashboard tức thì, không đợi đầy bộ đệm của Producer.
                producer.flush()
                
                # Bước 5: Kiểm soát tần suất gọi API
                time.sleep(POLL_INTERVAL)
                
            except Exception as e:
                logger.error(f"Lỗi trong chu kỳ xử lý: {e}")
                time.sleep(POLL_INTERVAL)
                
    except KeyboardInterrupt:
        # Xử lý khi người dùng nhấn Ctrl+C hoặc Docker gửi tín hiệu dừng (SIGINT)
        logger.info("Đang dừng script (Graceful Shutdown)...")
    finally:
        producer.flush(timeout=5)

        # HTTP Session: Đóng Connection Pool để giải phóng các socket TCP đang mở.
        http_session.close()
        logger.info("Đã giải phóng toàn bộ tài nguyên kết nối.")

if __name__ == '__main__':
    run_producer()