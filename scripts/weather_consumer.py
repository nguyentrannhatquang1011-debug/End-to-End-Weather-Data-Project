import os
import json
import logging
from typing import Dict, Any, Optional

from confluent_kafka import Consumer, KafkaError, Message
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
from data_transformer import transform_weather_data

# Tải các biến môi trường từ file .env
load_dotenv()

# =============================================================================
# CẤU HÌNH LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("WeatherConsumer")

class WeatherConsumer:
    """Lớp xử lý việc tiêu thụ dữ liệu từ Kafka và ghi vào InfluxDB.

    Attributes:
        influx_url (str): Địa chỉ kết nối tới InfluxDB.
        influx_token (str): Token xác thực API InfluxDB.
        influx_org (str): Tên tổ chức trong InfluxDB.
        influx_bucket (str): Tên bucket lưu trữ dữ liệu thời tiết.
        kafka_conf (dict): Cấu hình kết nối và hành vi của Kafka Consumer.
        topic (str): Tên topic Kafka cần lắng nghe.
    """

    def __init__(self) -> None:
        """
        Khởi tạo cấu hình Kafka và InfluxDB từ biến môi trường.
        """
        # Cấu hình InfluxDB
        self.influx_url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
        self.influx_token = os.getenv("INFLUXDB_TOKEN")
        self.influx_org = os.getenv("INFLUXDB_ORG", "weather_org")
        self.influx_bucket = os.getenv("INFLUXDB_BUCKET", "weather_data")

        # Cấu hình Kafka
        self.kafka_conf = {
            'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            'group.id': 'weather-consumer-group',
            'auto.offset.reset': 'earliest',
            # 'enable.auto.commit': False: Rất quan trọng để đảm bảo At-least-once delivery.
            # Chúng ta sẽ chỉ commit offset sau khi đã ghi thành công vào InfluxDB.
            'enable.auto.commit': False
        }
        self.topic = "weather_live"
        
        # Khởi tạo clients
        self.influx_client = InfluxDBClient(url=self.influx_url, token=self.influx_token, org=self.influx_org)
        # Sử dụng SYNCHRONOUS để đảm bảo write success xảy ra trước khi commit Kafka offset
        self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
        self.consumer = Consumer(self.kafka_conf)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    def write_to_influx(self, data: Dict[str, Any]) -> None:
        """
        Ghi dữ liệu thời tiết vào InfluxDB với cơ chế Retry.

        Args:
            data: Dữ liệu JSON nhận được từ Kafka Producer.

        Workflow:
        1. Trích xuất các trường dữ liệu từ payload của Open-Meteo.
        2. Tạo đối tượng Point của InfluxDB.
        3. Thực hiện ghi dữ liệu đồng bộ vào Bucket.

        Note:
            Sử dụng WriteOptions=SYNCHRONOUS để đảm bảo dữ liệu thực sự đã nằm trong DB
            trước khi Consumer tiến hành commit offset tới Kafka.
        """
        # SỬ DỤNG MODULE CHUNG ĐỂ BIẾN ĐỔI DỮ LIỆU
        cleaned_data = transform_weather_data(data)
        
        if not cleaned_data:
            logger.warning("Không thể transform dữ liệu thời tiết. Bỏ qua.")
            return

        # Tạo Point dữ liệu cho InfluxDB
        # Measurement: weather_metrics
        # Tags: location (Hà Nội theo tọa độ của Producer)
        # Fields: temperature, windspeed, weathercode
        point = Point("weather_metrics") \
            .tag("location", cleaned_data["station_id"]) \
            .field("temperature", cleaned_data["temperature"]) \
            .field("windspeed", cleaned_data["windspeed"]) \
            .field("winddirection", cleaned_data["winddirection"]) \
            .field("weathercode", cleaned_data["weathercode"]) \
            .time(cleaned_data["timestamp"])

        self.write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=point)
        logger.info(f"Đã ghi thành công dữ liệu mốc {cleaned_data['timestamp']} vào InfluxDB.")

    def process_messages(self) -> None:
        """
        Vòng lặp chính để lắng nghe và xử lý tin nhắn từ Kafka.
        
        Workflow:
        1. Subscribe vào topic.
        2. Poll tin nhắn liên tục.
        3. Parse dữ liệu từ bytes sang JSON.
        4. Gọi hàm ghi vào InfluxDB.
        5. Commit offset thủ công sau khi xử lý thành công.
        """
        try:
            self.consumer.subscribe([self.topic])
            logger.info(f"Consumer đã được đăng ký và bắt đầu lắng nghe topic: {self.topic}")

            while True:
                # Poll dữ liệu từ Kafka Broker. Timeout 1.0s giúp script không block 
                # hoàn toàn và có thể nhận tín hiệu dừng hệ thống (Ctrl+C).
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    # Nếu gặp lỗi cuối Partition (EOF), tiếp tục poll để chờ dữ liệu mới.
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Lỗi Kafka: {msg.error()}")
                        break

                # =============================================================
                # WORKFLOW XỬ LÝ TIN NHẮN (At-least-once delivery)
                # =============================================================
                try:
                    # 1. Giải mã bytes từ Kafka thành JSON
                    payload = json.loads(msg.value().decode('utf-8'))
                    
                    # 2. Ghi vào InfluxDB (Hàm này có retry tự động nếu DB tạm thời lỗi)
                    # Lưu ý: Theo tiêu chuẩn DRY, logic mapping Point này nên được 
                    # gọi từ module data_transformer.py trong tương lai.
                    self.write_to_influx(payload)

                    # Bước quan trọng: Commit offset thủ công (At-least-once delivery)
                    # Điều này đảm bảo nếu script chết trước khi ghi vào DB, 
                    # tin nhắn sẽ được đọc lại ở lần chạy sau.
                    self.consumer.commit(message=msg, asynchronous=False)
                    
                except json.JSONDecodeError:
                    logger.error("Không thể decode JSON từ tin nhắn Kafka.")
                except Exception as e:
                    logger.error(f"Lỗi khi xử lý tin nhắn: {e}")
                    # Trong môi trường streaming, ta có thể chọn skip tin nhắn lỗi 
                    # để tránh làm tắc nghẽn luồng (Dead Letter Queue logic có thể áp dụng sau).

        except KeyboardInterrupt:
            logger.info("Đang dừng Consumer (Graceful Shutdown)...")
        finally:
            self.close()

    def close(self) -> None:
        """Giải phóng tài nguyên kết nối."""
        if self.consumer:
            self.consumer.close()
        if self.influx_client:
            self.influx_client.close()
        logger.info("Đã đóng toàn bộ kết nối Kafka và InfluxDB.")

def run_weather_consumer() -> None:
    """
    Khởi tạo và chạy luồng thực thi chính của consumer.
    
    Workflow:
    1. Kiểm tra sự tồn tại của Token InfluxDB (tránh crash giữa chừng).
    2. Khởi tạo instance và bắt đầu vòng lặp xử lý.
    """
    if not os.getenv("INFLUXDB_TOKEN"):
        logger.error("KHÔNG TÌM THẤY INFLUXDB_TOKEN. Consumer sẽ không thể ghi dữ liệu.")
        return

    weather_processor = WeatherConsumer()
    weather_processor.process_messages()

if __name__ == '__main__':
    run_weather_consumer()