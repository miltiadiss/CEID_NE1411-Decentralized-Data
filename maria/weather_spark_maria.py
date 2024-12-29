from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

# Define schema for weather data based on the producer's schema
weather_schema = StructType([
    StructField("main", StructType([
        StructField("temp", DoubleType(), True),
        StructField("humidity", DoubleType(), True)
    ])),
    StructField("weather", ArrayType(StructType([
        StructField("description", StringType(), True)
    ])), True),
    StructField("wind", StructType([
        StructField("speed", DoubleType(), True)
    ]))
])

def create_spark_session():
    """Create a Spark session with necessary configurations"""
    return SparkSession \
        .builder \
        .appName("WeatherDataProcessor") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.streaming.kafka.maxRatePerPartition", "100") \
        .getOrCreate()

def process_weather_data(spark):
    """Process weather data stream from Kafka"""
    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "weather_topic") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON data and add timestamp
    parsed_df = df.select(
        current_timestamp().alias("processing_time"),
        from_json(col("value").cast("string"), weather_schema).alias("weather_data")
    ).select("processing_time", "weather_data.*")

    # Extract and transform relevant fields
    weather_processed = parsed_df.select(
        col("processing_time"),
        col("main.temp").alias("temperature"),
        col("main.humidity").alias("humidity"),
        col("weather")[0]["description"].alias("weather_description"),
        col("wind.speed").alias("wind_speed")
    )

    # Add temperature in Celsius (the API returns Kelvin)
    weather_final = weather_processed.withColumn(
        "temperature_celsius", 
        col("temperature") - 273.15
    )

    # Write the raw data stream to console
    console_query = weather_final \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="1 minute") \
        .start()

    # Calculate windowed aggregations separately
    windowed_aggs = weather_final \
        .withWatermark("processing_time", "1 hour") \
        .groupBy(
            window("processing_time", "1 hour")
        ) \
        .agg({
            "temperature_celsius": "avg",
            "humidity": "avg",
            "wind_speed": "avg"
        })

    # Write aggregations to console
    agg_query = windowed_aggs \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="1 minute") \
        .start()

    # Wait for termination
    console_query.awaitTermination()
    agg_query.awaitTermination()

def main():
    """Main function to run the Spark streaming job"""
    spark = create_spark_session()
    try:
        process_weather_data(spark)
    except KeyboardInterrupt:
        print("Stopping the Spark streaming job...")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
