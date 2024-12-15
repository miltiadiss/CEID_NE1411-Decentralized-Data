from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col, expr, window

# Kafka and Spark configuration
KAFKA_BROKER = "localhost:9092"
WEATHER_TOPIC = "weather_topic"
STATION_INFO_TOPIC = "station_info_topic"
STATION_STATUS_TOPIC = "station_status_topic"

# Database configuration
DB_URL = "jdbc:postgresql://localhost:5432/analytics"
DB_TABLE = "hourly_usage_summary"
DB_PROPERTIES = {
    "user": "your_username",
    "password": "your_password",
    "driver": "org.postgresql.Driver"
}

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkSQLAnalytics") \
    .config("spark.jars", "/path/to/postgresql.jar") \
    .getOrCreate()

# Define schemas
weather_schema = StructType() \
    .add("main", StructType()
         .add("temp", DoubleType())
         .add("humidity", DoubleType())) \
    .add("weather", StringType()) \
    .add("wind", StructType()
         .add("speed", DoubleType())) \
    .add("timestamp", TimestampType())

station_info_schema = StructType() \
    .add("data", StructType()
         .add("stations", StructType()
              .add("station_id", StringType())
              .add("name", StringType())
              .add("lat", DoubleType())
              .add("lon", DoubleType()))) \
    .add("last_updated", IntegerType())

station_status_schema = StructType() \
    .add("data", StructType()
         .add("stations", StructType()
              .add("station_id", StringType())
              .add("num_bikes_available", IntegerType())
              .add("num_docks_available", IntegerType()))) \
    .add("last_updated", IntegerType())

# Function to process Kafka streams
def process_stream(topic, schema):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load() \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*")

# Read and parse streams
weather_stream = process_stream(WEATHER_TOPIC, weather_schema) \
    .withColumnRenamed("main.temp", "temperature") \
    .withColumnRenamed("main.humidity", "humidity") \
    .withColumnRenamed("wind.speed", "wind_speed") \
    .withColumn("timestamp", expr("current_timestamp()"))

station_info_stream = process_stream(STATION_INFO_TOPIC, station_info_schema) \
    .withColumn("timestamp", expr("current_timestamp()"))

station_status_stream = process_stream(STATION_STATUS_TOPIC, station_status_schema) \
    .withColumn("timestamp", expr("current_timestamp()"))

# Create temporary views for SparkSQL
weather_stream.createOrReplaceTempView("weather")
station_info_stream.createOrReplaceTempView("station_info")
station_status_stream.createOrReplaceTempView("station_status")

# Query 1: Overall System Utilization with Weather Conditions
overall_utilization_query = """
    SELECT 
        w.temperature,
        w.humidity,
        w.wind_speed,
        SUM(ss.num_bikes_available) AS total_bikes_available,
        SUM(ss.num_docks_available) AS total_docks_available,
        SUM(ss.num_bikes_available) / SUM(ss.num_bikes_available + ss.num_docks_available) AS system_utilization_rate,
        ss.timestamp
    FROM 
        weather AS w
    JOIN 
        station_status AS ss
    ON 
        w.timestamp = ss.timestamp
    GROUP BY 
        w.temperature, w.humidity, w.wind_speed, ss.timestamp
"""
overall_utilization = spark.sql(overall_utilization_query)

# Query 2: Hourly Usage Summaries
hourly_usage_query = """
    SELECT 
        station_id, 
        SUM(num_bikes_available) AS total_bikes_in_use, 
        SUM(num_docks_available) AS total_docks_available,
        SUM(num_bikes_available) / SUM(num_bikes_available + num_docks_available) AS utilization_rate,
        window(timestamp, '1 hour') AS time_window
    FROM 
        station_status
    GROUP BY 
        station_id, window(timestamp, '1 hour')
"""
hourly_usage_summary = spark.sql(hourly_usage_query)

# Write overall utilization to console
overall_utilization.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Write hourly usage summaries to database
hourly_usage_summary.writeStream \
    .foreachBatch(lambda df, epoch_id: df.write.jdbc(DB_URL, DB_TABLE, mode="append", properties=DB_PROPERTIES)) \
    .outputMode("append") \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()
