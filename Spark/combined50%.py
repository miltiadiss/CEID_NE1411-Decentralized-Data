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
])

# Schema για το station_status_topic
stationStatusSchema = StructType([
    StructField("station_id", StringType(), True),
    StructField("num_bikes_available", IntegerType(), True),
    StructField("num_docks_available", IntegerType(), True),
])

dataSchema1 = StructType([StructField("stations", ArrayType(stationInfoSchema), True)])
dataSchema2 = StructType([StructField("stations", ArrayType(stationStatusSchema), True)])

kafkaSchema1 = StructType([StructField("last_updated", IntegerType(), True), StructField("ttl", IntegerType(), True), StructField("data", dataSchema1, True)])
kafkaSchema2 = StructType([StructField("last_updated", IntegerType(), True), StructField("ttl", IntegerType(), True), StructField("data", dataSchema2, True)])

# Διαβάζουμε δεδομένα από το station_info_topic
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
        lit("Dubai").alias("city_name"),
        current_timestamp().alias("timestamp")  # Add timestamp from station info
    )

# Διαβάζουμε δεδομένα από το station_status_topic
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

# Join τα stationInfoDF και stationStatusDF με βάση το station_id
combinedDF = stationInfoDF.join(stationStatusDF, on="station_id", how="inner") \
    .withColumn("utilization_rate",
                (col("num_bikes_available") / col("capacity")) * 100)  # Υπολογισμός του ποσοστού χρησιμοποίησης

# Διαβάζουμε τα δεδομένα καιρού από Kafka
weatherStreamDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_topic") \
    .load()

# Παίρνουμε τα δεδομένα καιρού
weatherDF = weatherStreamDF.selectExpr("CAST(value AS STRING) as json") \
    .select(
        get_json_object(col("json"), "$.name").alias("city_name"),
        get_json_object(col("json"), "$.main.temp").cast("double").alias("temperature"),
        get_json_object(col("json"), "$.wind.speed").cast("double").alias("wind_speed"),
        get_json_object(col("json"), "$.clouds.all").cast("int").alias("cloudiness"),
        get_json_object(col("json"), "$.rain.1h").cast("double").alias("precipitation")
    )

# Κάνουμε join weatherDF με combinedDF χρησιμοποιώντας το city_name
finalDF = combinedDF.join(weatherDF, on="city_name", how="inner")

# Γράφουμε το τελικό αποτέλεσμα στην κονσόλα
query = finalDF.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Περιμένουμε την ολοκλήρωση της εκτέλεσης
query.awaitTermination()

"""                                                                                                                                                                                                                                                                                                           testc3.py                                                                                                                                                                                                                                                                                                                    from pyspark.sql import SparkSession
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
])

# Schema για το station_status_topic
stationStatusSchema = StructType([
    StructField("station_id", StringType(), True),
    StructField("num_bikes_available", IntegerType(), True),
    StructField("num_docks_available", IntegerType(), True),
])

dataSchema1 = StructType([StructField("stations", ArrayType(stationInfoSchema), True)])
dataSchema2 = StructType([StructField("stations", ArrayType(stationStatusSchema), True)])

kafkaSchema1 = StructType([StructField("last_updated", IntegerType(), True), StructField("ttl", IntegerType(), True), StructField("data", dataSchema1, True)])
kafkaSchema2 = StructType([StructField("last_updated", IntegerType(), True), StructField("ttl", IntegerType(), True), StructField("data", dataSchema2, True)])

# Διαβάζουμε δεδομένα από το station_info_topic
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
        lit("Dubai").alias("city_name"),
        current_timestamp().alias("timestamp")  # Add timestamp from station info
    )

# Διαβάζουμε δεδομένα από το station_status_topic
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

# Join τα stationInfoDF και stationStatusDF με βάση το station_id
combinedDF = stationInfoDF.join(stationStatusDF, on="station_id", how="inner") \
    .withColumn("utilization_rate",
                (col("num_bikes_available") / col("capacity")) * 100)  # Υπολογισμός του ποσοστού χρησιμοποίησης

# Διαβάζουμε τα δεδομένα καιρού από Kafka
weatherSchema = StructType([
    StructField("city_name", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("cloudiness", IntegerType(), True),
])

weatherDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_topic") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), weatherSchema).alias("weather")) \
    .select("weather.*")

# Κάνουμε join weatherDF με combinedDF χρησιμοποιώντας το city_name
finalDF = combinedDF.join(weatherDF, on="city_name", how="inner")

# Υπολογισμός συνολικών στατιστικών χρήσης
aggregatedDF = finalDF\
.withWatermark("timestamp", "30 minutes") \
.groupBy(
    window(col("timestamp"),"30 minutes"),  # Ανάλυση ανά ώρα
    col("city_name"),
    col("temperature"),
    col("wind_speed"),
    col("cloudiness")
).agg(
    avg("utilization_rate").alias("average_docking_station_utilisation"),
    max("utilization_rate").alias("max_docking_station_utilisation"),
    min("utilization_rate").alias("min_docking_station_utilisation"),
    stddev("utilization_rate").alias("std_dev_docking_station_utilisation")
).select(
    col("window.start").alias("timestamp"),
    col("city_name"),
    col("temperature"),
    col("wind_speed"),
    col("cloudiness"),
    col("average_docking_station_utilisation"),
    col("max_docking_station_utilisation"),
    col("min_docking_station_utilisation"),
    col("std_dev_docking_station_utilisation")
)

# Γράφουμε το τελικό αποτέλεσμα στην κονσόλα
query = aggregatedDF.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Περιμένουμε την ολοκλήρωση της εκτέλεσης
query.awaitTermination()


"""
