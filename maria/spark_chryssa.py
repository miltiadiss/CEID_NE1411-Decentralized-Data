from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import datetime

# Create SparkSession
spark = SparkSession.builder \
    .appName("KafkaMultiTopicStreaming") \
    .getOrCreate()

# [Previous schema definitions remain the same...]
stationInfoSchema = StructType([
    StructField("station_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("capacity", IntegerType(), True),
])

stationStatusSchema = StructType([
    StructField("station_id", StringType(), True),
    StructField("num_bikes_available", IntegerType(), True),
    StructField("num_docks_available", IntegerType(), True),
])

dataSchema1 = StructType([StructField("stations", ArrayType(stationInfoSchema), True)])
dataSchema2 = StructType([StructField("stations", ArrayType(stationStatusSchema), True)])

kafkaSchema1 = StructType([StructField("last_updated", IntegerType(), True), StructField("ttl", IntegerType(), True), StructField("data", dataSchema1, True)])
kafkaSchema2 = StructType([StructField("last_updated", IntegerType(), True), StructField("ttl", IntegerType(), True), StructField("data", dataSchema2, True)])

# [Previous DataFrame definitions remain the same...]
# Read station information
stationInfoDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "station_info_topic") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), kafkaSchema1).alias("data")) \
    .select(explode("data.data.stations").alias("station")) \
    .select(
        col("station.station_id").alias("station_id"),
        col("station.name").alias("name"),
        col("station.lat").alias("latitude"),
        col("station.lon").alias("longitude"),
        col("station.capacity").alias("capacity"),
        lit("Dubai").alias("city_name")
    )

# Read station status with watermark
stationStatusDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "station_status_topic") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json", "timestamp as status_timestamp") \
    .select(from_json(col("json"), kafkaSchema2).alias("data"), col("status_timestamp")) \
    .select(explode("data.data.stations").alias("station"), col("status_timestamp")) \
    .select(
        col("station.station_id").alias("station_id"),
        col("station.num_bikes_available").alias("num_bikes_available"),
        col("station.num_docks_available").alias("num_docks_available"),
        col("status_timestamp")
    ) \
    .withWatermark("status_timestamp", "5 minutes")

# Read weather data with watermark
weatherDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_topic") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json", "timestamp as weather_timestamp") \
    .select(
        get_json_object(col("json"), "$.name").alias("city_name"),
        get_json_object(col("json"), "$.main.temp").cast("double").alias("temperature"),
        get_json_object(col("json"), "$.wind.speed").cast("double").alias("wind_speed"),
        get_json_object(col("json"), "$.clouds.all").cast("int").alias("cloudiness"),
        get_json_object(col("json"), "$.rain.1h").cast("double").alias("precipitation"),
        col("weather_timestamp")
    ) \
    .withWatermark("weather_timestamp", "5 minutes")

# Join and process data
stationMetricsDF = stationInfoDF.join(stationStatusDF, "station_id") \
    .withColumn("utilization_rate",
                (col("num_bikes_available") / col("capacity")) * 100) \
    .withColumn("window", window(col("status_timestamp"), "10 minutes"))

windowedStatsDF = stationMetricsDF.groupBy("city_name", "window") \
    .agg(
        avg("utilization_rate").alias("average_docking_station_utilisation"),
        max("utilization_rate").alias("max_docking_station_utilisation"),
        min("utilization_rate").alias("min_docking_station_utilisation"),
        stddev("utilization_rate").alias("std_dev_docking_station_utilisation")
    )

finalDF = windowedStatsDF.join(
    weatherDF,
    (windowedStatsDF.city_name == weatherDF.city_name) &
    (windowedStatsDF.window.start <= weatherDF.weather_timestamp) &
    (windowedStatsDF.window.end > weatherDF.weather_timestamp)
) \
.select(
    col("window.start").alias("timestamp"),
    windowedStatsDF.city_name,
    col("temperature"),
    col("wind_speed"),
    col("precipitation"),
    col("cloudiness"),
    col("average_docking_station_utilisation"),
    col("max_docking_station_utilisation"),
    col("min_docking_station_utilisation"),
    col("std_dev_docking_station_utilisation")
)

# Function to write DataFrame to CSV with timestamp in filename
def writeToCSV(df, epoch_id):
    # Get current timestamp for filename
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Write to CSV
    df.write \
        .mode("append") \
        .csv(f"bike_usage_statistics/date={timestamp[:8]}", header=True)

# Write to console for monitoring
consoleQuery = finalDF.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime='10 seconds') \
    .option("truncate", "false") \
    .start()

# Write to CSV
csvQuery = finalDF.writeStream \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .foreachBatch(writeToCSV) \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()
