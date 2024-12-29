from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime

# Δημιουργία SparkSession
spark = SparkSession.builder \
    .appName("StationMetricsAndRawData") \
    .getOrCreate()

# Ορισμός σχημάτων
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

# Ανάγνωση δεδομένων από Kafka
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
        col("station.capacity").alias("capacity")
    )

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
    .withWatermark("status_timestamp", "30 minutes")

# Join για υπολογισμό utilization rate
stationMetricsDF = stationInfoDF.join(stationStatusDF, "station_id") \
    .withColumn("utilization_rate",
                (col("num_bikes_available") / col("capacity")) * 100) \
    .withColumn("window", window(col("status_timestamp"), "30 minutes"))

# Υπολογισμός metrics ανά μισή ώρα
windowedStatsDF = stationMetricsDF.groupBy("window") \
    .agg(
        avg("utilization_rate").alias("average_docking_station_utilisation"),
        max("utilization_rate").alias("max_docking_station_utilisation"),
        min("utilization_rate").alias("min_docking_station_utilisation"),
        stddev("utilization_rate").alias("std_dev_docking_station_utilisation")
    ) \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")  # Αφαιρούμε τη στήλη window που προκαλεί το πρόβλημα

# Αποθήκευση σε CSV των metrics
def writeToCSV(df, epoch_id):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    df.write \
        .mode("append") \
        .csv(f"station_metrics/date={timestamp[:8]}", header=True)

csvMetricsQuery = windowedStatsDF.writeStream \
    .outputMode("append") \
    .trigger(processingTime='30 seconds') \
    .foreachBatch(writeToCSV) \
    .start()

# Αποθήκευση όλων των raw δεδομένων
rawDataDF = stationMetricsDF \
    .select(
        "station_id",
        "num_bikes_available",
        "num_docks_available",
        "status_timestamp",
        "utilization_rate"
    )

def writeRawToCSV(df, epoch_id):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    df.write \
        .mode("append") \
        .csv(f"raw_station_data/date={timestamp[:8]}", header=True)

csvRawQuery = rawDataDF.writeStream \
    .outputMode("append") \
    .trigger(processingTime='30 seconds') \
    .foreachBatch(writeRawToCSV) \
    .start()

# Αναμονή για τερματισμό
spark.streams.awaitAnyTermination()
