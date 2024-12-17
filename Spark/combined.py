from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Δημιουργία του SparkSession
spark = SparkSession.builder \
    .appName("KafkaMultiTopicStreaming") \
    .getOrCreate()

# Schema για το station_info_topic
stationInfoSchema = StructType([
    StructField("station_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("capacity", IntegerType(), True),
    StructField("is_charging_station", BooleanType(), True),
    StructField("nearby_distance", DoubleType(), True),
    StructField("rental_methods", ArrayType(StringType()), True),
    StructField("city_name", StringType(), True)  # Added city_name here for the join
])

# Schema για το station_status_topic
stationStatusSchema = StructType([
    StructField("station_id", StringType(), True),
    StructField("num_bikes_available", IntegerType(), True),
    StructField("num_docks_available", IntegerType(), True),
    StructField("num_bikes_disabled", IntegerType(), True),
    StructField("num_docks_disabled", IntegerType(), True),
    StructField("last_reported", IntegerType(), True),
    StructField("is_charging_station", BooleanType(), True),
    StructField("status", StringType(), True),
    StructField("is_installed", BooleanType(), True),
    StructField("is_renting", BooleanType(), True),
    StructField("is_returning", BooleanType(), True),
    StructField("traffic", DoubleType(), True)
])

dataSchema1 = StructType([
    StructField("stations", ArrayType(stationInfoSchema), True)
])

dataSchema2 = StructType([
    StructField("stations", ArrayType(stationStatusSchema), True)
])

kafkaSchema1 = StructType([
    StructField("last_updated", IntegerType(), True),
    StructField("ttl", IntegerType(), True),
    StructField("data", dataSchema1, True)
])

kafkaSchema2 = StructType([
    StructField("last_updated", IntegerType(), True),
    StructField("ttl", IntegerType(), True),
    StructField("data", dataSchema2, True)
])

# Διαβάζουμε δεδομένα από το station_info_topic (station_info)
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
        lit("Dubai").alias("city_name")  # Ensure that city_name is part of station info
    )

# Διαβάζουμε δεδομένα από το station_status_topic (station_status)
stationStatusDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "station_status_topic") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), kafkaSchema2).alias("data")) \
    .select(explode("data.data.stations").alias("station")) \
    .select(
        col("station.station_id").alias("station_id"),
        col("station.num_bikes_available").alias("num_bikes_available"),
        col("station.num_docks_available").alias("num_docks_available")
    )

# Κάνουμε join με βάση το station_id
combinedDF = stationInfoDF.join(stationStatusDF, on="station_id", how="inner")

# Διαβάζουμε δεδομένα από Kafka για τον καιρό
kafkaStreamDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_topic") \
    .load()

# Μετατροπή του value σε String και εξαγωγή topic για διαχωρισμό
baseDF = kafkaStreamDF.selectExpr("CAST(value AS STRING) as json", "topic")

# Διαχωρισμός ανά topic
weatherDF = baseDF.filter(col("topic") == "weather_topic") \
    .select(
        current_timestamp().alias("timestamp"),
        get_json_object(col("json"), "$.name").alias("city_name"),
        get_json_object(col("json"), "$.main.temp").cast("double").alias("temperature"),
        get_json_object(col("json"), "$.wind.speed").cast("double").alias("wind_speed"),
        get_json_object(col("json"), "$.clouds.all").cast("int").alias("cloudiness"),
        get_json_object(col("json"), "$.rain.1h").cast("double").alias("precipitation")
    )

# Join weather data with station data based on city_name
finalDF = combinedDF.join(weatherDF, on="city_name", how="inner")

# Calculate overall system utilization and weather metrics every 5 minutes
finalDF_with_metrics = finalDF \
    .withWatermark("timestamp", "1 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("city_name"),
        col("temperature"),
        col("wind_speed"),
        col("precipitation"),
        col("cloudiness")
    ) \
    .agg(
        avg(col("num_bikes_available") / col("capacity")).alias("average_docking_station_utilisation"),
        max(col("num_bikes_available") / col("capacity")).alias("max_docking_station_utilisation"),
        min(col("num_bikes_available") / col("capacity")).alias("min_docking_station_utilisation"),
        stddev(col("num_bikes_available") / col("capacity")).alias("std_dev_docking_station_utilisation")
    )

# Final streaming query to write output to console
query = finalDF_with_metrics.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Wait for termination
query.awaitTermination()
