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
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AQIProducer")

# =============================================================================
# TỐI ƯU HÓA CONNECTION POOLING (AQI)
# =============================================================================
http_session = requests.Session()
adapter = HTTPAdapter(pool_connections=2, pool_maxsize=10)
http_session.mount("https://", adapter)
http_session.mount("http://", adapter)

def delivery_report(err: Optional[Exception], msg: Any) -> None:
    """
    Hàm callback báo cáo trạng thái gửi tin nhắn tới Kafka.
    
    Workflow:
    1. Được gọi khi Kafka Broker phản hồi (xác nhận đã nhận hoặc báo lỗi).
    2. Nếu err không None: Ghi log lỗi để phục vụ monitoring.
    3. Nếu thành công: Ghi log thông tin Topic và Partition.
    """
    if err is not None:
        logger.error(f"Gửi tin nhắn AQI thất bại: {err}")
    else:
        logger.info(f"AQI - Gửi thành công tới {msg.topic()} [Partition: {msg.partition()}]")

# =============================================================================
# HÀM TRÍCH XUẤT DỮ LIỆU AQI
# =============================================================================
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True
)
def get_aqi_data(session: requests.Session) -> Dict[str, Any]:
    """
    Gọi API AQICN để lấy chỉ số chất lượng không khí hiện tại của Hà Nội.
    
    Cơ chế:
    - Sử dụng tenacity.retry: Tự động thử lại nếu API lỗi (5xx, Network timeout).
    - Exponential Backoff: Đợi 2s, 4s, 8s giữa các lần thử để tránh overload API.
    - requests.Session: Tái sử dụng kết nối TCP để giảm overhead handshake SSL.

    Args:
        session: Đối tượng requests.Session dùng chung cho toàn bộ tiến trình.

    Returns:
        Dict[str, Any]: Dữ liệu AQI dạng JSON.
        
    Raises:
        Exception: Khi API trả về trạng thái không phải 'ok' hoặc lỗi HTTP.
    """
    api_key = os.getenv("AQICN_API_KEY")
    if not api_key:
        raise ValueError("AQICN_API_KEY không tồn tại trong file .env")

    # Endpoint của AQICN cho khu vực Hà Nội
    url = f"https://api.waqi.info/feed/A477292/?token={api_key}"
    
    # Sử dụng session truyền vào thay vì block 'with requests.Session()'
    response = session.get(url, timeout=15)
    response.raise_for_status()
    
    data = response.json()
    # API AQICN luôn trả về 200 OK ngay cả khi token sai, cần kiểm tra field 'status'
    if data.get("status") != "ok":
        error_msg = data.get("data", "Unknown error")
        raise Exception(f"AQICN API Error: {error_msg}")
        
    return data

def run_producer() -> None:
    """
    Luồng xử lý chính: Khởi tạo Producer và chạy vòng lặp streaming dữ liệu AQI.
    
    Workflow chi tiết:
    1. Đọc cấu hình Kafka từ biến môi trường.
    2. Khởi tạo Producer với acks='all' (đảm bảo an toàn dữ liệu tối đa).
    3. Chạy vòng lặp:
        a. Lấy dữ liệu từ API.
        b. Serialize JSON và Encode sang UTF-8.
        c. Đẩy vào topic 'aqi_live'.
        d. Flush để đảm bảo tin nhắn đi ngay (Low Latency).
        e. Nghỉ 60 giây (Dữ liệu không khí thường cập nhật chậm hơn thời tiết).
    """
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = "aqi_live"
    
    # Tần suất lấy dữ liệu AQI: 300-600 giây vì AQI cập nhật rất chậm (theo giờ)
    POLL_INTERVAL = 200 

    # Cấu hình Producer
    producer_config = {
        'bootstrap.servers': kafka_servers,
        'client.id': 'aqi-producer',
        'acks': 'all',
        'compression.type': 'snappy' # Nén dữ liệu để tiết kiệm băng thông và Disk Space
    }
    
    try:
        producer = Producer(producer_config)
        logger.info(f"Đã khởi tạo AQI Producer kết nối tới {kafka_servers}")

        # Khởi tạo biến lưu trạng thái để chống trùng dữ liệu (Deduplication)
        # API AQICN thường cập nhật dữ liệu theo giờ, nên việc check timestamp là bắt buộc
        last_data_time = None

        while True:
            try:
                # Bước 1: Trích xuất
                aqi_payload = get_aqi_data(http_session)
                
                # Lấy timestamp từ API response (Cấu trúc: data -> time -> v hoặc iso)
                # Chúng ta dùng 'v' (epoch time) để so sánh số nguyên cho chính xác
                current_data_time = aqi_payload.get('data', {}).get('time', {}).get('v')

                if current_data_time == last_data_time:
                    logger.info(f"AQI chưa có bản tin mới (Last update: {current_data_time}). Đang đợi...")
                    time.sleep(POLL_INTERVAL)
                    continue
                
                # Nếu có dữ liệu mới, cập nhật flag và tiến hành produce
                last_data_time = current_data_time
                
                # Bước 2: Biến đổi sang Byte
                message_str = json.dumps(aqi_payload)
                message_bytes = message_str.encode('utf-8')
                
                # Bước 3: Đưa vào hàng đợi Kafka
                producer.produce(
                    topic=topic,
                    value=message_bytes,
                    callback=delivery_report
                )
                
                # Bước 4: Ép gửi ngay lập tức
                producer.flush()
                
                # Nghỉ 200s
                # Chỉ số AQI thường được các trạm quan trắc cập nhật theo giờ hoặc mỗi 30-60 phút.
                time.sleep(POLL_INTERVAL)
                
            except Exception as e:
                # khi một Exception được bắt (catch) bởi khối except, chương trình coi như lỗi đó đã được xử lý xong. 
                # Nếu bên trong khối except bạn không có lệnh break (để thoát vòng lặp) hoặc raise (để đẩy lỗi ra ngoài cấp cao hơn), 
                # thì vòng lặp sẽ tiếp tục chu kỳ tiếp theo một cách bình thường.
                logger.error(f"Lỗi trong chu kỳ AQI: {e}")
                time.sleep(POLL_INTERVAL) # Nghỉ 5 phút nếu có lỗi
                
    except KeyboardInterrupt:
        logger.info("Đang đóng AQI Producer...")
    finally:
        # Đảm bảo các tin nhắn AQI cuối cùng được ACK bởi Kafka Broker
        producer.flush(timeout=5)
        
        # Giải phóng Connection Pool của requests.Session
        http_session.close()
        logger.info("Đã đóng các kết nối AQI an toàn.")

if __name__ == '__main__':
    run_producer()