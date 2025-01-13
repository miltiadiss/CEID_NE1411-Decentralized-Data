# Δημιουργία SparkSession
spark = SparkSession.builder \
    .appName("ReadBikeStats") \
    .getOrCreate()

# Διάβασμα όλων των CSV από όλες τις ημέρες
all_data_spark = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("bike_usage_statistics/date=*")

# Εμφάνιση όλων των δεδομένων
all_data_list = all_data_spark.collect()

# Εκτύπωση όλων των γραμμών
for row in all_data_list:
    print(row)
