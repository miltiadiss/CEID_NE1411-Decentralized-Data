from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col, avg, count, window, stddev, max, min

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
WEATHER_TOPIC = "weather_topic"
STATION_INFO_TOPIC = "station_info_topic"
STATION_STATUS_TOPIC = "station_status_topic"
CITY_NAME = "YourCity"  # Replace with actual city name

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .getOrCreate()

# Define schemas for incoming JSON data
weather_schema = StructType() \
    .add("main", StructType()
         .add("temp", DoubleType())
         .add("humidity", DoubleType())) \
    .add("weather", StringType()) \
    .add("wind", StructType()
         .add("speed", DoubleType()))

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

# Process each topic
weather_stream = process_stream(WEATHER_TOPIC, weather_schema)
station_info_stream = process_stream(STATION_INFO_TOPIC, station_info_schema)
station_status_stream = process_stream(STATION_STATUS_TOPIC, station_status_schema)

# Join station info with station status
station_info_with_status = station_info_stream \
    .join(station_status_stream, "station_id") \
    .select("station_id", "name", "lat", "lon", "num_bikes_available", "num_docks_available")

# Calculate utilization rate per station
station_info_with_status = station_info_with_status \
    .withColumn("utilization_rate", col("num_bikes_available") / (col("num_bikes_available") + col("num_docks_available")))

# Correlate weather conditions with bike usage
weather_station_info = station_info_with_status \
    .join(weather_stream, "station_id") \
    .select(
        "station_id", "name", "lat", "lon", "num_bikes_available", "num_docks_available", 
        "main.temp", "main.humidity", "wind.speed", "utilization_rate"
    )

# Generate hourly usage summaries
hourly_usage_summary = weather_station_info \
    .withWatermark("timestamp", "1 hour") \
    .groupBy(window(col("timestamp"), "1 hour"), "station_id", "name") \
    .agg(
        avg("num_bikes_available").alias("avg_bikes_available"),
        avg("num_docks_available").alias("avg_docks_available"),
        avg("utilization_rate").alias("avg_utilization_rate"),
        avg("temp").alias("avg_temp"),
        avg("humidity").alias("avg_humidity"),
        avg("wind.speed").alias("avg_wind_speed"),
        max("utilization_rate").alias("max_utilization_rate"),
        min("utilization_rate").alias("min_utilization_rate"),
        stddev("utilization_rate").alias("std_dev_utilization_rate")
    )

# Overall city utilization dataframe
city_utilization = hourly_usage_summary \
    .groupBy("window") \
    .agg(
        avg("avg_utilization_rate").alias("city_avg_utilization"),
        max("max_utilization_rate").alias("city_max_utilization"),
        min("min_utilization_rate").alias("city_min_utilization"),
        stddev("std_dev_utilization_rate").alias("city_std_dev_utilization"),
        avg("avg_temp").alias("city_avg_temp"),
        avg("avg_humidity").alias("city_avg_humidity"),
        avg("avg_wind_speed").alias("city_avg_wind_speed")
    )

city_utilization = city_utilization \
    .withColumn("city_name", col("city_name"))

# Add required fields to hourly usage summary for storage
final_hourly_summary = hourly_usage_summary \
    .select(
        col("window.start").alias("timestamp"),
        col("city_name"),
        col("avg_temp").alias("temperature"),
        col("avg_wind_speed").alias("wind_speed"),
        col("avg_utilization_rate").alias("average_docking_station_utilisation"),
        col("max_utilization_rate").alias("max_docking_station_utilisation"),
        col("min_utilization_rate").alias("min_docking_station_utilisation"),
        col("std_dev_utilization_rate").alias("std_dev_docking_station_utilisation")
    )

# Save the hourly usage summary to a CSV
final_hourly_summary.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "output/hourly_usage_summary") \
    .option("checkpointLocation", "output/checkpoint") \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()
