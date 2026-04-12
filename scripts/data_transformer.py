import pandas as pd
from typing import Dict, Any, Optional
from datetime import datetime

def transform_weather_data(raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Chuẩn hóa dữ liệu thời tiết từ Open-Meteo API.
    
    Args:
        raw_data: Dictionary chứa dữ liệu thô từ API.
        
    Returns:
        Dictionary đã làm sạch hoặc None nếu dữ liệu không hợp lệ.
    """
    current = raw_data.get("current_weather", {})
    if not current:
        return None
    
    # Open-Meteo trả về format ISO8601 thiếu giây và offset (VD: 2024-03-27T10:00)
    # Ta cần thêm ':00' (giây) và '+07:00' (múi giờ Hà Nội) để InfluxDB hiểu đúng UTC.
    raw_time = current.get("time")
    formatted_time = f"{raw_time}:00+07:00" if raw_time else None

    return {
        "station_id": "HaNoi",
        "timestamp": formatted_time,
        "temperature": float(current.get("temperature", 0)),
        "windspeed": float(current.get("windspeed", 0)),
        "winddirection": float(current.get("winddirection", 0)),
        "weathercode": int(current.get("weathercode", 0))
    }

def transform_aqi_data(raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Chuẩn hóa dữ liệu AQI từ AQICN API.
    
    Args:
        raw_data: Dictionary chứa dữ liệu thô từ API.
        
    Returns:
        Dictionary đã làm sạch hoặc None nếu dữ liệu không hợp lệ.
    """
    data = raw_data.get("data", {})
    if not data or not data.get("time"):
        return None
        
    # AQICN trả về epoch time (giây). 
    # Sử dụng pandas để chuyển đổi sang múi giờ Hà Nội một cách chuẩn xác.
    timestamp_v = data.get("time", {}).get("v")
    if timestamp_v:
        ts_obj = pd.to_datetime(timestamp_v, unit='s').tz_localize('UTC').tz_convert('Asia/Bangkok')
        formatted_time = ts_obj.strftime('%Y-%m-%dT%H:%M:%S+07:00')
    else:
        return None
    
    return {
        "station_id": "HaNoi",
        "station_name": data.get("city", {}).get("name", "Unknown Station"),
        "timestamp": formatted_time,
        "aqi": float(data.get("aqi", 0)),
        "lat": float(data.get("city", {}).get("geo", [0, 0])[0]),
        "lon": float(data.get("city", {}).get("geo", [0, 0])[1])
    }