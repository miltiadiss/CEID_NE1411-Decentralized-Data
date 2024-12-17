from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, ArrayType

# Δημιουργία του SparkSession
spark = SparkSession.builder \
    .appName("KafkaStationStatusStreaming") \
    .getOrCreate()

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

dataSchema = StructType([
    StructField("stations", ArrayType(stationStatusSchema), True)
])

kafkaSchema = StructType([
    StructField("last_updated", IntegerType(), True),
    StructField("ttl", IntegerType(), True),
    StructField("data", dataSchema, True)
])

# Διαβάζουμε δεδομένα από Kafka
kafkaStreamDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "station_status_topic") \
    .load()

# Μετατροπή του value σε String και parsing του JSON
parsedDF = kafkaStreamDF.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), kafkaSchema).alias("data"))

# Ξεδιπλώνουμε τη λίστα των σταθμών
stationStatusDF = parsedDF.select(explode("data.data.stations").alias("station")) \
    .select(
        col("station.station_id").alias("station_id"),
        col("station.num_bikes_available").alias("num_bikes_available"),
        col("station.num_docks_available").alias("num_docks_available")
    )

# Γράφουμε τα δεδομένα στην κονσόλα
query = stationStatusDF.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
