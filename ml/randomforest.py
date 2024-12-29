from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, unix_timestamp
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt

# Step 1: Create a SparkSession
spark = SparkSession.builder \
    .appName("BikeUtilizationPrediction") \
    .getOrCreate()

# Step 2: Load the dataset
file_path = "/home/unix/ml/dataset.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Debug: Display schema and sample data
df.printSchema()
df.show(5)

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

# Compute MSE (Mean Squared Error)
mse_evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mse")
mse = mse_evaluator.evaluate(predictions)
print(f"Mean Squared Error (MSE): {mse}")

# Step 9: Save the Trained Model
model_path = "/home/unix/ml/random_forest_model"
rf_model.save(model_path)

print(f"Model saved to {model_path}")

# Step 10: Export Predictions for Visualization
predictions_pd = predictions.select("label", "prediction").toPandas()

# Create a chart comparing actual vs predicted values
plt.figure(figsize=(10, 6))
plt.plot(predictions_pd['label'], label="Actual", marker='o', linestyle='dashed', alpha=0.6)
plt.plot(predictions_pd['prediction'], label="Predicted", marker='x', alpha=0.6)
plt.title("Actual vs Predicted Utilization")
plt.xlabel("Sample Index")
plt.ylabel("Utilization")
plt.legend()
plt.show()

# Create a bar chart for MSE
plt.figure(figsize=(5, 5))
plt.bar(["MSE"], [mse], alpha=0.7, color='blue')
plt.title("Model Mean Squared Error")
plt.ylabel("MSE Value")
plt.show()

