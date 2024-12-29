from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, unix_timestamp
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
import pandas as pd
import os

# Create a SparkSession
spark = SparkSession.builder \
    .appName("BikeUtilizationPrediction") \
    .getOrCreate()

# Load the dataset
file_path = "/home/unix/ml/dataset.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Preprocessing and Feature Engineering
df = df.withColumn("hour", hour(unix_timestamp("timestamp").cast("timestamp")))
df = df.withColumn("dayofweek", dayofweek(unix_timestamp("timestamp").cast("timestamp")))
df = df.fillna({"precipitation": 0})

# Select Features and Label
feature_columns = ['temperature', 'wind_speed', 'precipitation', 'cloudiness', 'hour', 'dayofweek']
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
df = assembler.transform(df)
df = df.select(col("features"), col("average_docking_station_utilisation").alias("label"))

# Train-Test Split
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

# Build and Train the Random Forest Model
rf = RandomForestRegressor(featuresCol="features", labelCol="label", numTrees=100)
rf_model = rf.fit(train_data)

# Make Predictions
predictions = rf_model.transform(test_data)

# Evaluate the Model
mse_evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mse")
mse = mse_evaluator.evaluate(predictions)

# Create output directory if it doesn't exist
output_dir = "/home/unix/ml"
os.makedirs(output_dir, exist_ok=True)

# Save predictions to CSV
predictions_pd = predictions.select("label", "prediction").toPandas()
predictions_csv_path = os.path.join(output_dir, "predictions.csv")
predictions_pd.to_csv(predictions_csv_path, index=False)

# Create and save the MSE chart
plt.figure(figsize=(8, 6))
plt.bar(["MSE"], [mse], alpha=0.7, color='blue')
plt.title("Model Mean Squared Error")
plt.ylabel("MSE Value")
plt.tight_layout()

# Save the MSE chart
mse_chart_path = os.path.join(output_dir, "mse_chart.png")
plt.savefig(mse_chart_path, dpi=300, bbox_inches='tight')
plt.close()
