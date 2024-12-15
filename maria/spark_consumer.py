from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col

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

# Output streams to console
weather_query = weather_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

station_info_query = station_info_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

station_status_query = station_status_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()
