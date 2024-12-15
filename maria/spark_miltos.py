from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col, avg, max, min, stddev, window, lit

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
WEATHER_TOPIC = "weather_topic"
STATION_INFO_TOPIC = "station_info_topic"
STATION_STATUS_TOPIC = "station_status_topic"

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
         .add("speed", DoubleType())) \
    .add("clouds", StructType()
         .add("all", IntegerType())) \
    .add("precipitation", StringType())  # Assuming precipitation is a string (e.g., "light", "heavy")

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
    # Read stream from Kafka topic
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    # Deserialize JSON messages
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
    .select("station_id", "name", "lat", "lon", "num_bikes_available", "num_docks_available", 
            "temp", "humidity", "wind.speed", "clouds.all", "precipitation", "utilization_rate", 
            lit("Dubai").alias("city_name"))

# Generate hourly usage summaries with required fields
hourly_usage_summary = weather_station_info \
    .withWatermark("timestamp", "1 hour") \
    .groupBy(window(col("timestamp"), "1 hour"), "city_name") \
    .agg(
        avg("num_bikes_available").alias("avg_bikes_available"),
        avg("num_docks_available").alias("avg_docks_available"),
        avg("utilization_rate").alias("avg_utilization_rate"),
        max("utilization_rate").alias("max_utilization_rate"),
        min("utilization_rate").alias("min_utilization_rate"),
        stddev("utilization_rate").alias("std_dev_utilization_rate"),
        avg("temp").alias("avg_temp"),
        avg("humidity").alias("avg_humidity"),
        avg("wind.speed").alias("avg_wind_speed"),
        avg("clouds.all").alias("avg_cloudiness"),
        avg("precipitation").alias("avg_precipitation")
    )

# Output hourly usage summary to console (for debugging and inspection)
hourly_usage_query = hourly_usage_summary.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Example: Saving the hourly usage summary to a database (e.g., PostgreSQL)
hourly_usage_query = hourly_usage_summary.writeStream \
    .foreachBatch(lambda df, epoch_id: 
                  df.write.format("jdbc")
                  .option("url", "jdbc:postgresql://localhost:5432/bike_usage")
                  .option("dbtable", "hourly_usage_summary")
                  .option("user", "username")
                  .option("password", "password")
                  .mode("append")
                  .save()) \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()
