import os
import argparse
import logging
import json
import pandas as pd
import duckdb
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

# Tải biến môi trường
load_dotenv()

# =============================================================================
# CẤU HÌNH LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("StreamingBackfiller")

class StreamingBackfiller:
    """Lớp xử lý hồi phục dữ liệu (Backfill) cho InfluxDB từ Lakehouse hoặc API."""

    def __init__(self) -> None:
        """Khởi tạo kết nối tới các dịch vụ InfluxDB và cấu hình DuckDB."""
        # InfluxDB Config
        self.influx_url = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
        self.influx_token = os.getenv("INFLUXDB_TOKEN")
        self.influx_org = os.getenv("INFLUXDB_ORG", "weather_org")
        self.influx_bucket = os.getenv("INFLUXDB_BUCKET", "weather_data")

        # MinIO/S3 Config cho DuckDB
        self.s3_user = os.getenv("MINIO_ROOT_USER")
        self.s3_pass = os.getenv("MINIO_ROOT_PASSWORD")
        self.s3_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", "")

        # Khởi tạo InfluxDB Client
        self.influx_client = InfluxDBClient(url=self.influx_url, token=self.influx_token, org=self.influx_org)
        self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)

        # Khởi tạo DuckDB để scan Iceberg
        self.duck_conn = duckdb.connect(database=':memory:')
        self.duck_conn.execute("INSTALL httpfs; LOAD httpfs;")
        self.duck_conn.execute("INSTALL iceberg; LOAD iceberg;")
        self.duck_conn.execute(f"SET s3_endpoint='{self.s3_endpoint}';")
        self.duck_conn.execute(f"SET s3_access_key_id='{self.s3_user}';")
        self.duck_conn.execute(f"SET s3_secret_access_key='{self.s3_pass}';")
        self.duck_conn.execute("SET s3_use_ssl=false; SET s3_url_style='path';")

    def _filter_forecast(self, df: pd.DataFrame, time_col: str) -> pd.DataFrame:
        """Loại bỏ dữ liệu dự báo (Forecast) - Chỉ giữ lại dữ liệu <= hiện tại."""
        now = datetime.now()
        # Đảm bảo cột thời gian là datetime object
        df[time_col] = pd.to_datetime(df[time_col])
        return df[df[time_col] <= now]

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def fetch_from_api(self, data_type: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Lấy dữ liệu từ API Open-Meteo trong trường hợp dữ liệu chưa có trong Lakehouse."""
        logger.info(f"Đang gọi API Open-Meteo để lấy bù dữ liệu {data_type} từ {start_date} đến {end_date}...")
        
        if data_type == "weather":
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": 21.0285, "longitude": 105.8542,
                "start_date": start_date, "end_date": end_date,
                "hourly": "temperature_2m,wind_speed_10m,wind_direction_10m,weather_code",
                "timezone": "Asia/Bangkok"
            }
            node = "hourly"
        else:  # aqi
            url = "https://air-quality-api.open-meteo.com/v1/air-quality"
            params = {
                "latitude": 21.0285, "longitude": 105.8542,
                "start_date": start_date, "end_date": end_date,
                "hourly": "pm2_5,pm10", "timezone": "Asia/Bangkok"
            }
            node = "hourly"

        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()
        data = response.json().get(node, {})
        
        if not data: return pd.DataFrame()
        
        df = pd.DataFrame(data)
        df.rename(columns={"time": "timestamp", "temperature_2m": "temperature", 
                           "wind_speed_10m": "windspeed", "wind_direction_10m": "winddirection",
                           "weather_code": "weathercode"}, inplace=True)
        
        if data_type == "aqi":
            # Tính toán AQI đơn giản từ pm2_5 và pm10 như logic tầng Silver
            df['aqi'] = df[['pm2_5', 'pm10']].max(axis=1)
            
        return df

    def fetch_from_iceberg(self, data_type: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Truy vấn dữ liệu từ Silver Layer (Iceberg) trên MinIO."""
        table_name = "weather_silver" if data_type == "weather" else "aqi_silver"
        s3_path = f"s3://silver/weather_db/{table_name}"
        
        query = f"""
            SELECT * FROM iceberg_scan('{s3_path}')
            WHERE CAST(timestamp AS DATE) BETWEEN '{start_date}' AND '{end_date}'
        """
        try:
            logger.info(f"Đang quét Lakehouse (SSOT) cho {data_type}...")
            return self.duck_conn.execute(query).df()
        except Exception as e:
            logger.warning(f"Không thể đọc từ Lakehouse (Có thể bảng chưa tồn tại): {e}")
            return pd.DataFrame()

    def write_to_influx(self, df: pd.DataFrame, data_type: str) -> None:
        """Chuyển đổi DataFrame thành InfluxDB Points và ghi đồng bộ."""
        if df.empty:
            logger.info("Không có dữ liệu để ghi vào InfluxDB.")
            return

        # Lọc bỏ forecast
        df = self._filter_forecast(df, "timestamp")
        
        points = []
        measurement = "weather_metrics" if data_type == "weather" else "aqi_metrics"
        
        for _, row in df.iterrows():
            p = Point(measurement).tag("location", "HaNoi").time(row['timestamp'])
            
            if data_type == "weather":
                p.field("temperature", float(row['temperature'])) \
                 .field("windspeed", float(row['windspeed'])) \
                 .field("winddirection", float(row['winddirection'])) \
                 .field("weathercode", int(row['weathercode']))
            else:
                p.field("aqi", float(row['aqi']))
            
            points.append(p)

        if points:
            self.write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=points)
            logger.info(f"Đã ghi bù {len(points)} bản ghi {data_type} vào InfluxDB.")

    def run_backfill(self, data_type: str, start_date: str, end_date: str) -> None:
        """Workflow chính: Kiểm tra Lakehouse trước, nếu thiếu gọi API."""
        # 1. Thử lấy từ SSOT (Iceberg)
        df = self.fetch_from_iceberg(data_type, start_date, end_date)
        
        # 2. Kiểm tra nếu dữ liệu trong Lakehouse bị thiếu (so với dải ngày yêu cầu)
        # Đơn giản hóa: Nếu là ngày hiện tại, ta luôn gọi thêm API để lấy Intraday gap
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        if df.empty or end_date >= today_str:
            logger.info("Dữ liệu Lakehouse không đủ hoặc cần lấy gap trong ngày. Gọi API dự phòng...")
            api_df = self.fetch_from_api(data_type, start_date, end_date)
            
            if not df.empty:
                # Gộp và xóa trùng dựa trên timestamp
                df = pd.concat([df, api_df]).drop_duplicates(subset=['timestamp'], keep='last')
            else:
                df = api_df

        # 3. Ghi vào InfluxDB
        self.write_to_influx(df, data_type)

    def close(self) -> None:
        """Giải phóng tài nguyên."""
        self.influx_client.close()
        self.duck_conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Streaming Backfill Script (Lakehouse to InfluxDB)")
    parser.add_argument("--type", choices=["weather", "aqi"], required=True)
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    
    args = parser.parse_args()
    
    backfiller = StreamingBackfiller()
    try:
        backfiller.run_backfill(args.type, args.start_date, args.end_date)
    finally:
        backfiller.close()