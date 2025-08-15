import os
import time
import requests
from kafka import KafkaProducer
import json
from datetime import datetime


API_KEY = os.getenv("API_KEY", "c3f68666c60c4a6b9ab100520251208")
CITY = os.getenv("CITY", "Ho Chi Minh City")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
API_URL = "http://api.weatherapi.com/v1/current.json?key={API_KEY}&q={CITY}&aqi=no"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
    max_block_ms=10000
)

def fetch_and_produce():
    try:
        response = requests.get(API_URL.format(API_KEY=API_KEY, CITY=CITY))
        if response.status_code == 200:
            weather_data = response.json()
            record = {
                "city": weather_data["location"]["name"],
                "country": weather_data["location"]["country"],
                "temperature": weather_data["current"]["temp_c"],
                "condition": weather_data["current"]["condition"]["text"],
                "humidity": weather_data["current"]["humidity"],
                "wind_speed": weather_data["current"]["wind_kph"],
                "precipitation": weather_data["current"]["precip_mm"],
                "cloud": weather_data["current"]["cloud"],
                "timestamp": datetime.now().isoformat()
            }
            producer.send("weather_topic", record)
            producer.flush()
            print(f"Successfully sent data: {record}")
        else:
            print(f"API request failed with status code: {response.status_code}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    while True:
        fetch_and_produce()
        time.sleep(15)  
