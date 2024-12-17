from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, ArrayType

# Δημιουργία του SparkSession
spark = SparkSession.builder \
    .appName("KafkaMultiTopicStreaming") \
    .getOrCreate()

# Schema για το JSON
stationSchema = StructType([
    StructField("station_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("capacity", IntegerType(), True),
    StructField("is_charging_station", BooleanType(), True),
    StructField("nearby_distance", DoubleType(), True),
    StructField("rental_methods", ArrayType(StringType()), True)
])

dataSchema = StructType([
    StructField("stations", ArrayType(stationSchema), True)
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
    .option("subscribe", "station_info_topic") \
    .load()

# Μετατροπή του value σε String και parsing του JSON
parsedDF = kafkaStreamDF.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), kafkaSchema).alias("data"))

# Ξεδιπλώνουμε τη λίστα των σταθμών
stationsDF = parsedDF.select(explode("data.data.stations").alias("station")) \
    .select(
        col("station.station_id").alias("station_id"),
        col("station.name").alias("name"),
        col("station.lat").alias("latitude"),
        col("station.lon").alias("longitude"),
        col("station.capacity").alias("capacity"),
        col("station.is_charging_station").alias("is_charging_station"),
        col("station.nearby_distance").alias("nearby_distance"),
        col("station.rental_methods").alias("rental_methods")
    )

# Γράφουμε τα δεδομένα στην κονσόλα
query = stationsDF.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
