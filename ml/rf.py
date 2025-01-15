from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql.types import StructType, StructField, FloatType, TimestampType, StringType, IntegerType
from pyspark.sql.functions import col, mean, to_timestamp
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from datetime import timedelta

# Initialize Spark session
spark = SparkSession.builder.appName("Bike Utilization Prediction").getOrCreate()

# Define schema for the dataset
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("wind_speed", FloatType(), True),
    StructField("precipitation", FloatType(), True),
    StructField("cloudiness", IntegerType(), True),
    StructField("average_docking_station_utilisation", FloatType(), True),
    StructField("max_docking_station_utilisation", FloatType(), True),
    StructField("min_docking_station_utilisation", FloatType(), True),
    StructField("std_dev_docking_station_utilisation", FloatType(), True)
])

# Load dataset
file_path = '/home/debian/spark-3.5.3-bin-hadoop3/spark_data.csv'  # Path to the CSV file
bike_data = spark.read.csv(file_path, header=True, schema=schema)

# Data Preprocessing
for column in ['temperature', 'wind_speed', 'precipitation', 'cloudiness']:
    mean_value = bike_data.select(mean(col(column))).collect()[0][0]
    bike_data = bike_data.fillna({column: mean_value})

bike_data = bike_data.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# Feature Scaling
feature_columns = ['temperature', 'wind_speed', 'precipitation', 'cloudiness']
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data = assembler.transform(bike_data)

scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
scaler_model = scaler.fit(data)
data = scaler_model.transform(data)

data = data.select("scaled_features", "average_docking_station_utilisation")

# Split dataset
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

# Train Random Forest Regressor
rf_regressor = RandomForestRegressor(
    featuresCol="scaled_features",
    labelCol="average_docking_station_utilisation",
    numTrees=500,
    maxDepth=30,
    minInstancesPerNode=5,
    maxBins=32,
    featureSubsetStrategy="auto",
    subsamplingRate=0.8
)
rf_model = rf_regressor.fit(train_data)

# Evaluate the model on training data
train_predictions = rf_model.transform(train_data)

# Convert predictions to Pandas DataFrame
train_predictions_df = train_predictions.select("prediction", "average_docking_station_utilisation").toPandas()

# Calculate metrics for each prediction
train_predictions_df["absolute_error"] = abs(train_predictions_df["prediction"] - train_predictions_df["average_docking_station_utilisation"])
train_predictions_df["squared_error"] = (train_predictions_df["prediction"] - train_predictions_df["average_docking_station_utilisation"]) ** 2

# Cumulative metrics calculations
train_predictions_df["cumulative_rmse"] = (train_predictions_df["squared_error"].expanding().mean()) ** 0.5
train_predictions_df["cumulative_mae"] = train_predictions_df["absolute_error"].expanding().mean()
train_predictions_df["cumulative_r2"] = 1 - (
    train_predictions_df["squared_error"].expanding().sum()
    / ((train_predictions_df["average_docking_station_utilisation"] - train_predictions_df["average_docking_station_utilisation"].mean()) ** 2).sum()
)

# Plot training metrics
plt.figure(figsize=(14, 8))

# Plot RMSE
plt.plot(train_predictions_df.index, train_predictions_df["cumulative_rmse"], label="RMSE", color="blue", linewidth=2)

# Plot MAE
plt.plot(train_predictions_df.index, train_predictions_df["cumulative_mae"], label="MAE", color="orange", linewidth=2)

# Plot R²
plt.plot(train_predictions_df.index, train_predictions_df["cumulative_r2"], label="R²", color="green", linewidth=2)

# Add labels, title, and legend
plt.xlabel("Training Data Samples")
plt.ylabel("Metric Value")
plt.title("Training Metrics Across All Predictions")
plt.legend()
plt.grid(True, linestyle="--", alpha=0.7)
plt.tight_layout()

# Show the plot
plt.show()

# Get the last row and prepare next hour input
last_row = bike_data.orderBy("timestamp", ascending=False).limit(1).collect()[0]
current_timestamp = last_row['timestamp']
next_timestamp = current_timestamp + timedelta(hours=1)

# Define schema for the next hour prediction
next_hour_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("city_name", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("wind_speed", FloatType(), True),
    StructField("precipitation", FloatType(), True),
    StructField("cloudiness", FloatType(), True)
])

# Create next hour data with updated timestamp
next_hour_data = spark.createDataFrame([(
    next_timestamp,
    last_row.city_name,
    float(last_row.temperature + 1.0),  # Example: assume 1 degree increase
    float(last_row.wind_speed),
    float(last_row.precipitation) if last_row.precipitation is not None else 0.0,
    float(last_row.cloudiness)
)], schema=next_hour_schema)

# Prepare features for prediction
next_hour_features = assembler.transform(next_hour_data)

# Scale the features for the next hour
next_hour_scaled = scaler_model.transform(next_hour_features)

# Predict for next hour
next_hour_prediction = rf_model.transform(next_hour_scaled)

# Extract and display the prediction
next_hour_result = next_hour_prediction.select(
    "timestamp",
    "city_name",
    "temperature",
    "wind_speed",
    "precipitation",
    "cloudiness",
    "prediction"
)
next_hour_result.show()
