# RealTimeWeatherProcessor

Pipeline xử lý dữ liệu thời tiết thời gian thực sử dụng **Apache Kafka**, **Spark Streaming** và **PostgreSQL**. Dự án thu thập dữ liệu thời tiết từ **WeatherAPI**, truyền qua Kafka, xử lý bằng Spark và lưu kết quả vào PostgreSQL để phân tích.

## Mô tả

Dự án này xây dựng một pipeline xử lý dữ liệu thời gian thực, lấy dữ liệu thời tiết (nhiệt độ, độ ẩm, tốc độ gió, v.v.) từ API WeatherAPI, gửi vào Kafka topic, sau đó sử dụng Spark Streaming để đọc, xử lý và lưu dữ liệu vào cơ sở dữ liệu PostgreSQL. Pipeline được triển khai trong môi trường Docker để đảm bảo tính di động và dễ dàng triển khai.

### Công nghệ sử dụng
- **Apache Kafka**: Hàng đợi tin nhắn để truyền dữ liệu thời tiết.
- **Apache Spark**: Xử lý dữ liệu thời gian thực với Spark Streaming.
- **PostgreSQL**: Lưu trữ dữ liệu thời tiết đã xử lý.
- **WeatherAPI**: Nguồn dữ liệu thời tiết.
- **Docker**: Container hóa các dịch vụ.
- **Python**: Ngôn ngữ chính cho producer và consumer.

## Cấu trúc thư mục
```
RealTimeWeatherProcessor/
├── docker-compose.yml      # Cấu hình Docker cho Kafka, Zookeeper, PostgreSQL, Spark
├── producer.py             # Script lấy dữ liệu từ WeatherAPI và gửi vào Kafka
├── spark-apps/             # Thư mục chứa Spark job
│   ├── consumer.py         # Script Spark xử lý dữ liệu từ Kafka và lưu vào PostgreSQL
```

## Yêu cầu
- **Docker** và **Docker Compose** đã được cài đặt.
- Python 3.8+ (cho `producer.py`).
- Tài khoản WeatherAPI để lấy `API_KEY`.

## Cài đặt và chạy

### 1. Clone repository
```bash
git clone https://github.com/NghiaAi/RealTime_Weather_Processor.git
cd RealTimeWeatherProcessor
```

### 2. Cấu hình
- Mở file `producer.py` để cập nhật `API_KEY` và `CITY` (nếu cần):
  ```python
  API_KEY = "your_weather_api_key"
  CITY = "Ho Chi Minh City"
  ```
- Mở file `spark-apps/consumer.py` để kiểm tra các cấu hình Kafka (`kafka:9092`) và PostgreSQL.

### 3. Khởi động các dịch vụ
Chạy các container (Zookeeper, Kafka, PostgreSQL, Spark) bằng Docker Compose:
```bash
docker-compose up -d
```

### 4. Tạo bảng PostgreSQL
Tạo bảng `weather_data` để lưu dữ liệu:
```bash
docker exec -it postgres psql -U user -d data_db -c "CREATE TABLE weather_data (city VARCHAR, country VARCHAR, temperature DOUBLE PRECISION, condition VARCHAR, humidity INTEGER, wind_speed DOUBLE PRECISION, precipitation DOUBLE PRECISION, cloud INTEGER, timestamp VARCHAR);"
```

### 5. Chạy producer
Chạy `producer.py` để lấy dữ liệu từ WeatherAPI và gửi vào Kafka:
```bash
python producer.py
```
Output sẽ hiển thị dữ liệu thời tiết được gửi vào topic `weather_topic`.

### 6. Chạy Spark consumer
Chạy `consumer.py` để xử lý dữ liệu từ Kafka và lưu vào PostgreSQL:
```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3 /opt/spark-apps/consumer.py
```
Output sẽ hiển thị trên console (dữ liệu thời tiết) và lưu vào bảng `weather_data`.

### 7. Kiểm tra dữ liệu
- **Kafka topic**:
  ```bash
  docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic weather_topic --from-beginning
  ```
- **PostgreSQL**:
  ```bash
  docker exec -it postgres psql -U user -d data_db -c "SELECT * FROM weather_data;"
  ```

## Kết quả
- Dữ liệu thời tiết (nhiệt độ, độ ẩm, tốc độ gió, v.v.) được gửi vào Kafka topic `weather_topic` mỗi 15 giây.
- Spark Streaming đọc dữ liệu, in ra console mỗi 5 giây, và lưu vào bảng `weather_data` trong PostgreSQL.
- Dữ liệu mẫu trong console:
  ```
  2025-08-15 14:37:00 - INFO - Processing batch ID: 0
  +----------------+-------+------------------+---------------+--------+----------+-------------+-----+---------------------+
  |city            |country|temperature       |condition      |humidity|wind_speed|precipitation|cloud|timestamp            |
  +----------------+-------+------------------+---------------+--------+----------+-------------+-----+---------------------+
  |Ho Chi Minh City|Vietnam|28.0              |Partly cloudy  |75      |10.8      |0.0          |50   |2025-08-15T14:30:00  |
  +----------------+-------+------------------+---------------+--------+----------+-------------+-----+---------------------+
  ```
