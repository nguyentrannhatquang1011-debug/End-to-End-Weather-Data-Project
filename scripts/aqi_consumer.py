import os
import json
import logging
from typing import Dict, Any, Optional

from confluent_kafka import Consumer, KafkaError
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
from data_transformer import transform_aqi_data

# Tải các biến môi trường từ file .env
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AQIConsumer")

class AQIConsumer:
    """Lớp xử lý việc tiêu thụ dữ liệu chất lượng không khí (AQI) từ Kafka và ghi vào InfluxDB.

    Lớp này thực hiện việc lắng nghe topic Kafka, giải mã dữ liệu JSON và ánh xạ 
    vào schema của InfluxDB theo mốc thời gian thực của trạm quan trắc.

    Attributes:
        influx_url (str): Địa chỉ kết nối tới InfluxDB.
        influx_token (str): Token xác thực API InfluxDB.
        influx_org (str): Tên tổ chức (Organization) trong InfluxDB.
        influx_bucket (str): Tên bucket lưu trữ dữ liệu AQI.
        kafka_conf (dict): Cấu hình kết nối và hành vi của Kafka Consumer.
        topic (str): Tên topic Kafka chứa luồng dữ liệu AQI.
    """

    def __init__(self) -> None:
        """
        Khởi tạo cấu hình Kafka và InfluxDB từ môi trường Container.
        """
        # Cấu hình kết nối InfluxDB (Lấy từ .env)
        self.influx_url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
        self.influx_token = os.getenv("INFLUXDB_TOKEN")
        self.influx_org = os.getenv("INFLUXDB_ORG", "weather_org")
        self.influx_bucket = os.getenv("INFLUXDB_BUCKET", "weather_data")

        # Cấu hình Kafka Consumer
        self.kafka_conf = {
            'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            'group.id': 'aqi-consumer-group',
            'auto.offset.reset': 'earliest',
            # enable.auto.commit=False để kiểm soát offset thủ công.
            # Điều này đảm bảo At-least-once delivery
            'enable.auto.commit': False
        }
        self.topic = "aqi_live"
        
        # Khởi tạo các Clients
        self.influx_client = InfluxDBClient(
            url=self.influx_url, 
            token=self.influx_token, 
            org=self.influx_org
        )
        # Sử dụng SYNCHRONOUS để đảm bảo write success xảy ra trước khi commit Kafka offset
        self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
        self.consumer = Consumer(self.kafka_conf)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    def write_to_influx(self, payload: Dict[str, Any]) -> None:
        """
        Biến đổi payload AQI và ghi vào InfluxDB với cơ chế Retry.

        Workflow:
        1. Trích xuất chỉ số AQI và timestamp từ cấu trúc lồng nhau của AQICN API.
        2. Ánh xạ vào đối tượng Point của InfluxDB.
        3. Thực hiện ghi dữ liệu.

        Args:
            payload: Dữ liệu JSON thô nhận được từ Kafka (aqi_producer.py).
        """
        # SỬ DỤNG MODULE CHUNG
        cleaned_data = transform_aqi_data(payload)
        
        if not cleaned_data:
            logger.warning("Không thể transform dữ liệu AQI. Bỏ qua.")
            return

        # Tạo Point dữ liệu cho Time-series DB
        point = Point("aqi_metrics") \
            .tag("location", cleaned_data["station_id"]) \
            .tag("station_name", cleaned_data["station_name"]) \
            .field("aqi", cleaned_data["aqi"]) \
            .time(cleaned_data["timestamp"])

        self.write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=point)
        logger.info(f"Ghi thành công AQI={cleaned_data['aqi']} tại mốc {cleaned_data['timestamp']} vào InfluxDB.")

    def process_messages(self) -> None:
        """
        Luồng thực thi chính: Đăng ký topic và bắt đầu vòng lặp tiêu thụ dữ liệu.

        Quy trình xử lý từng tin nhắn (Message Workflow):
        - Bước 1: Poll dữ liệu từ Kafka Broker.
        - Bước 2: Giải mã Byte sang JSON.
        - Bước 3: Ghi vào InfluxDB (Có retry).
        - Bước 4: Commit offset thủ công để xác nhận đã xử lý xong tin nhắn.
        """
        try:
            self.consumer.subscribe([self.topic])
            logger.info(f"Đã đăng ký tiêu thụ dữ liệu từ Topic: {self.topic}")

            while True:
                # Poll dữ liệu với timeout 1.0 giây để tránh block script khi stop container
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    # Nếu gặp lỗi cuối Partition (EOF), tiếp tục poll để chờ dữ liệu mới.
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Lỗi Kafka Consumer: {msg.error()}")
                        continue

                try:
                    # Giải mã mảng Byte sang JSON string và chuyển thành Dictionary
                    raw_value = msg.value().decode('utf-8')
                    payload = json.loads(raw_value)
                    
                    # Thực hiện ghi vào DB
                    self.write_to_influx(payload)

                    # COMMIT THỦ CÔNG: Chỉ chạy khi write_to_influx không ném lỗi
                    # asynchronous=False để đợi broker xác nhận đã lưu offset thành công
                    self.consumer.commit(message=msg, asynchronous=False)
                    
                except json.JSONDecodeError:
                    logger.error("Dữ liệu nhận được không đúng định dạng JSON.")
                except Exception as e:
                    logger.error(f"Lỗi không xác định khi xử lý tin nhắn AQI: {e}")

        except KeyboardInterrupt:
            logger.info("Nhận tín hiệu dừng từ người dùng. Đang đóng Consumer...")
        finally:
            self.close()

    def close(self) -> None:
        """
        Giải phóng tài nguyên kết nối để tối ưu hóa RAM và Port của hệ thống.
        """
        if self.consumer:
            self.consumer.close()
        if self.influx_client:
            self.influx_client.close()
        logger.info("Đã giải phóng an toàn kết nối Kafka và InfluxDB.")

def run_aqi_consumer() -> None:
    """
    Hàm khởi chạy ứng dụng Consumer.
    """
    # Kiểm tra điều kiện tiên quyết: Token xác thực InfluxDB
    if not os.getenv("INFLUXDB_TOKEN"):
        logger.critical("Thiếu INFLUXDB_TOKEN. Không thể khởi động Consumer.")
        return

    # Khởi tạo và thực thi luồng xử lý
    consumer_instance = AQIConsumer()
    consumer_instance.process_messages()

if __name__ == '__main__':
    # Điểm vào chính của script (Main Entry Point)
    try:
        run_aqi_consumer()
    except Exception as fatal_e:
        logger.critical(f"Lỗi chí tử gây sập Consumer: {fatal_e}")