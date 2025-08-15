from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Khởi tạo Spark session
spark = SparkSession.builder \
    .appName("WeatherStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema dữ liệu weather
schema = StructType([
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("condition", StringType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("cloud", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

# Đọc dữ liệu từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
weather_df = df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

# Ghi vào console để debug
console_query = weather_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 100) \
    .trigger(processingTime="5 seconds") \
    .start()

# Ghi vào PostgreSQL
postgres_query = weather_df.writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/data_db") \
        .option("dbtable", "weather_data") \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()) \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

# Chờ cả hai query hoàn tất
spark.streams.awaitAnyTermination()