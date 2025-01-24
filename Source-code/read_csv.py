from pyspark.sql import SparkSession

# Δημιουργία SparkSession
spark = SparkSession.builder \
    .appName("ReadBikeStats") \
    .getOrCreate()

# Διάβασμα όλων των CSV από όλες τις ημέρες
all_data_spark = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("bike_usage_statistics/date=*")

# Εμφάνιση των δεδομένων
all_data_spark.show(all_data_spark.count(), truncate=False)
