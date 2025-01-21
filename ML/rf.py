from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql.types import StructType, StructField, FloatType, TimestampType, StringType, IntegerType
from pyspark.sql.functions import col, mean, to_timestamp, lit, hour, dayofweek, when, lag
from pyspark.sql.window import Window
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
# Calculate mean for 'precipitation' column
mean_precipitation = bike_data.select(mean(col("precipitation"))).collect()[0][0]

# Ensure the value is of the correct type
mean_precipitation = float(mean_precipitation)

# Replace NULL values with the mean
bike_data = bike_data.fillna({"precipitation": mean_precipitation})

# Convert timestamp to TimestampType
bike_data = bike_data.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# Feature Scaling
feature_columns = [
    'temperature', 'wind_speed', 'precipitation', 'cloudiness'
]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data = assembler.transform(bike_data_cleaned)

scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
scaler_model = scaler.fit(data)
data = scaler_model.transform(data)

data = data.select("scaled_features", "average_docking_station_utilisation")

# Split dataset
train_data, validation_data = data.randomSplit([0.8, 0.2], seed=42)

# Train Random Forest Regressor
rf_regressor = RandomForestRegressor(
    featuresCol="scaled_features",
    labelCol="average_docking_station_utilisation",
    numTrees=100,
    maxDepth=10,
    minInstancesPerNode=5,
    maxBins=32,
    featureSubsetStrategy="sqrt",
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

# Evaluate the model on validation data
validation_predictions = rf_model.transform(validation_data)

# Convert predictions to Pandas DataFrame
validation_predictions_df = validation_predictions.select("prediction", "average_docking_station_utilisation").toPandas()

# Calculate metrics for each prediction
validation_predictions_df["absolute_error"] = abs(validation_predictions_df["prediction"] - validation_predictions_df["average_docking_station_utilisation"])
validationn_predictions_df["squared_error"] = (validation_predictions_df["prediction"] - validation_predictions_df["average_docking_station_utilisation"]) ** 2

# Cumulative metrics calculations
validation_predictions_df["cumulative_rmse"] = (validation_predictions_df["squared_error"].expanding().mean()) ** 0.5
validation_predictions_df["cumulative_mae"] = validation_predictions_df["absolute_error"].expanding().mean()
validation_predictions_df["cumulative_r2"] = 1 - (
    validation_predictions_df["squared_error"].expanding().sum()
    / ((validation_predictions_df["average_docking_station_utilisation"] - validation_predictions_df["average_docking_station_utilisation"].mean()) ** 2).sum()
)

# Plot training metrics
plt.figure(figsize=(14, 8))

# Plot RMSE
plt.plot(validation_predictions_df.index, train_predictions_df["cumulative_rmse"], label="RMSE", color="blue", linewidth=2)

# Plot MAE
plt.plot(validation_predictions_df.index, train_predictions_df["cumulative_mae"], label="MAE", color="orange", linewidth=2)

# Plot R²
plt.plot(validation_predictions_df.index, train_predictions_df["cumulative_r2"], label="R²", color="green", linewidth=2)

# Add labels, title, and legend
plt.xlabel("Validation Data Samples")
plt.ylabel("Metric Value")
plt.title("Validation Metrics Across All Predictions")
plt.legend()
plt.grid(True, linestyle="--", alpha=0.7)
plt.tight_layout()

# Show the plot
plt.show()

# Features Distribution
fig, axes = plt.subplots(2, 2, figsize=(14, 10))  # 2x2 grid for subplots

# Plot for temperature
sns.histplot(bike_data_cleaned.select("temperature").toPandas(), kde=True, ax=axes[0, 0])
axes[0, 0].set_title("Distribution of Temperature")
axes[0, 0].set_xlabel("Temperature")
axes[0, 0].set_ylabel("Frequency")

# Plot for wind_speed
sns.histplot(bike_data_cleaned.select("wind_speed").toPandas(), kde=True, ax=axes[0, 1])
axes[0, 1].set_title("Distribution of Wind Speed")
axes[0, 1].set_xlabel("Wind Speed")
axes[0, 1].set_ylabel("Frequency")

# Plot for precipitation
sns.histplot(bike_data_cleaned.select("precipitation").toPandas(), kde=True, ax=axes[1, 0])
axes[1, 0].set_title("Distribution of Precipitation")
axes[1, 0].set_xlabel("Precipitation")
axes[1, 0].set_ylabel("Frequency")

# Plot for average_docking_station_utilisation
sns.histplot(bike_data_cleaned.select("average_docking_station_utilisation").toPandas(), kde=True, ax=axes[1, 1])
axes[1, 1].set_title("Distribution of Average docking station utilisation")
axes[1, 1].set_xlabel("Average docking station utilisation")
axes[1, 1].set_ylabel("Frequency")

plt.tight_layout()  
plt.show()

# Correlation Heatmap
pandas_df = bike_data_cleaned.select("temperature", "wind_speed", "precipitation", "average_docking_station_utilisation").toPandas()
plt.figure(figsize=(10, 6))
sns.heatmap(pandas_df.corr(), annot=True, cmap="coolwarm", fmt=".2f", linewidths=0.5)
plt.title("Correlation Heatmap")
plt.show()

# Request user input for weather data for the next hour
city_name = input("Enter city name: ")
temperature = float(input("Enter the temperature (°C): "))
wind_speed = float(input("Enter wind speed (m/s): "))
precipitation = float(input("Enter precipitation (mm): "))
cloudiness = int(input("Enter cloudiness (percentage): "))

# Get the last timestamp for the prediction
last_row = bike_data.orderBy("timestamp", ascending=False).limit(1).collect()[0]
current_timestamp = last_row['timestamp']
next_timestamp = current_timestamp + timedelta(hours=1)

# Define schema for the next hour prediction using user inputs
next_hour_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("city_name", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("wind_speed", FloatType(), True),
    StructField("precipitation", FloatType(), True),
    StructField("cloudiness", FloatType(), True)
])

# Create next hour data with user inputs and the next timestamp
next_hour_data = spark.createDataFrame([(
    next_timestamp,
    city_name,
    temperature,
    wind_speed,
    precipitation,
    cloudiness
)], schema=next_hour_schema)

# Prepare features for prediction
next_hour_features = assembler.transform(next_hour_data)

# Scale the features for the next hour
next_hour_scaled = scaler_model.transform(next_hour_features)

# Predict for next hour
next_hour_prediction = rf_model.transform(next_hour_scaled)

# Extract and display the prediction
next_hour_result = next_hour_prediction.select("prediction").collect()
print(f"Predicted utilization for the next hour ({next_timestamp}) is: {next_hour_result[0]['prediction']}")
