from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col, avg, max, min, stddev, lit, current_timestamp, window

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
WEATHER_TOPIC = "weather_topic"
STATION_STATUS_TOPIC = "station_status_topic"

# Output directory for CSV files
OUTPUT_DIR = "/path/to/output/csv"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .getOrCreate()

# Define schemas for incoming JSON data
weather_schema = StructType() \
    .add("main", StructType()
         .add("temp", DoubleType())
         .add("humidity", DoubleType())) \
    .add("wind", StructType()
         .add("speed", DoubleType())) \
    .add("clouds", StructType()
         .add("all", IntegerType())) \
    .add("precipitation", StringType())

station_status_schema = StructType() \
    .add("data", StructType()
         .add("stations", StructType()
              .add("station_id", StringType())
              .add("num_bikes_available", IntegerType())
              .add("num_docks_available", IntegerType())))

# Function to process Kafka stream
def process_stream(topic, schema):
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    processed_stream = kafka_stream \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*")
    return processed_stream

# Process topics
weather_stream = process_stream(WEATHER_TOPIC, weather_schema)
station_status_stream = process_stream(STATION_STATUS_TOPIC, station_status_schema)

# Add timestamp and city_name to weather stream
weather_stream = weather_stream \
    .withColumn("timestamp", current_timestamp()) \
    .withColumn("city_name", lit("Dubai"))

# Calculate overall system utilization metrics
station_status_with_metrics = station_status_stream \
    .withColumn("utilization_rate", col("num_bikes_available") / 
                (col("num_bikes_available") + col("num_docks_available"))) \
    .select("utilization_rate", current_timestamp().alias("timestamp"))

# Aggregate metrics with weather data
overall_utilization = station_status_with_metrics \
    .join(weather_stream, "timestamp") \
    .withWatermark("timestamp", "1 hour") \
    .groupBy(window(col("timestamp"), "1 hour"), "city_name") \
    .agg(
        avg("utilization_rate").alias("avg_utilization_rate"),
        max("utilization_rate").alias("max_utilization_rate"),
        min("utilization_rate").alias("min_utilization_rate"),
        stddev("utilization_rate").alias("std_dev_utilization_rate"),
        avg("main.temp").alias("avg_temperature"),
        avg("wind.speed").alias("avg_wind_speed"),
        avg("clouds.all").alias("avg_cloudiness"),
        lit("precipitation").alias("precipitation")  # Placeholder
    )

# Save the aggregated data to CSV files
query_csv = overall_utilization.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", OUTPUT_DIR) \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()
