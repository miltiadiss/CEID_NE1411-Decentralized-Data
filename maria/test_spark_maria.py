from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime

# Δημιουργία SparkSession
spark = SparkSession.builder \
    .appName("StationMetricsAndWeatherData") \
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

weatherSchema = StructType([
    StructField("main", StructType([
        StructField("temp", DoubleType(), True),
        StructField("humidity", DoubleType(), True)
    ])),
    StructField("weather", ArrayType(StructType([
        StructField("description", StringType(), True)
    ]))),
    StructField("wind", StructType([
        StructField("speed", DoubleType(), True)
    ])),
    StructField("timestamp", TimestampType(), True)
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

weatherDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_topic") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), weatherSchema).alias("data")) \
    .select(
        col("data.main.temp").alias("temperature"),
        col("data.main.humidity").alias("humidity"),
        col("data.wind.speed").alias("wind_speed"),
        col("data.weather")[0]["description"].alias("weather_description"),
        from_unixtime(col("data.last_updated")).cast("timestamp").alias("weather_timestamp")  # Cast to TimestampType
    ) \
    .withWatermark("weather_timestamp", "1 hour")  # Apply watermarking


# Join για υπολογισμό utilization rate
stationMetricsDF = stationInfoDF.join(stationStatusDF, "station_id") \
    .withColumn("utilization_rate",
                (col("num_bikes_available") / col("capacity")) * 100) \
    .withColumn("window", window(col("status_timestamp"), "30 minutes"))

# Correlate weather data
correlatedDF = stationMetricsDF.join(
    weatherDF,
    stationMetricsDF.status_timestamp == weatherDF.weather_timestamp,
    "inner"
).select(
    stationMetricsDF.station_id,
    stationMetricsDF.utilization_rate,
    weatherDF.temperature,
    weatherDF.humidity,
    weatherDF.wind_speed,
    weatherDF.description,
    stationMetricsDF.status_timestamp
)

# Save weather data to CSV
def writeWeatherToCSV(df, epoch_id):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    df.write \
        .mode("append") \
        .csv(f"weather_data/date={timestamp[:8]}", header=True)

weatherQuery = weatherDF.writeStream \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .foreachBatch(writeWeatherToCSV) \
    .start()

# Save correlated data
def writeCorrelatedDataToCSV(df, epoch_id):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    df.write \
        .mode("append") \
        .csv(f"correlated_data/date={timestamp[:8]}", header=True)

correlationQuery = correlatedDF.writeStream \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .foreachBatch(writeCorrelatedDataToCSV) \
    .start()

# Await termination
spark.streams.awaitAnyTermination()
