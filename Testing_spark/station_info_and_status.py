from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, ArrayType

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
    StructField("rental_methods", ArrayType(StringType()), True)
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
        col("station.lon").alias("longitude")
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

# Γράφουμε τα δεδομένα στην κονσόλα
query = combinedDF.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
