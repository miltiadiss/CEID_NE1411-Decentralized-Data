# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, lit, unix_timestamp
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# Step 1: Create a SparkSession
spark = SparkSession.builder \
    .appName("BikeUtilizationPrediction") \
    .getOrCreate()

# Step 2: Load the dataset
file_path = "/mnt/data/part-00169-ab9c2382-bb04-48b8-a9b8-4b242fcee5c0-c000.csv"  # Replace with your file path
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Step 3: Preprocessing and Feature Engineering
# Handle timestamp to extract relevant time-based features
df = df.withColumn("hour", hour(unix_timestamp("timestamp").cast("timestamp")))  # Extract hour
df = df.withColumn("dayofweek", dayofweek(unix_timestamp("timestamp").cast("timestamp")))  # Extract day of the week

# Handle missing values (e.g., fill precipitation with 0 if NaN)
df = df.fillna({"precipitation": 0})

# Step 4: Select Features and Label
feature_columns = ['temperature', 'wind_speed', 'precipitation', 'cloudiness', 'hour', 'dayofweek']
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

df = assembler.transform(df)

# Select the features and target label for training
df = df.select(col("features"), col("average_docking_station_utilisation").alias("label"))

# Step 5: Train-Test Split
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

# Step 6: Build and Train the Random Forest Model
rf = RandomForestRegressor(featuresCol="features", labelCol="label", numTrees=100)
rf_model = rf.fit(train_data)

# Step 7: Make Predictions
predictions = rf_model.transform(test_data)

# Display a few predictions for inspection
print("Sample Predictions:")
predictions.select("features", "label", "prediction").show(10)

# Step 8: Evaluate the Model
evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE): {rmse}")

# Step 9: Save the Trained Model
model_path = "/path_to_save_model/random_forest_model"  # Replace with the desired path
rf_model.save(model_path)

print(f"Model saved to {model_path}")

# Additional: Print All Predictions as a Pandas DataFrame (Optional)
print("Exporting Predictions for Detailed Inspection...")
predictions_pd = predictions.select("label", "prediction").toPandas()
print(predictions_pd.head(10))  # Print the first 10 rows of predictions
