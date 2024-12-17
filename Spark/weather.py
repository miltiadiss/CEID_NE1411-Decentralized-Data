from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Δημιουργία του SparkSession
spark = SparkSession.builder \
    .appName("KafkaMultiTopicStreaming") \
    .getOrCreate()

# Διαβάζουμε δεδομένα από Kafka
kafkaStreamDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_topic") \
    .load()

# Μετατροπή του value σε String και εξαγωγή topic για διαχωρισμό
baseDF = kafkaStreamDF.selectExpr("CAST(value AS STRING) as json", "topic")

# Διαχωρισμός ανά topic

# 1. Weather DataFrame
weatherDF = baseDF.filter(col("topic") == "weather_topic") \
    .select(
        current_timestamp().alias("timestamp"),
        get_json_object(col("json"), "$.name").alias("city_name"),
        get_json_object(col("json"), "$.main.temp").cast("double").alias("temperature"),
        get_json_object(col("json"), "$.wind.speed").cast("double").alias("wind_speed"),
        get_json_object(col("json"), "$.clouds.all").cast("int").alias("cloudiness"),
        get_json_object(col("json"), "$.rain.1h").cast("double").alias("precipitation")
    )

# Weather Data
weatherQuery = weatherDF.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Περιμένουμε και τα τρία queries
weatherQuery.awaitTermination()
