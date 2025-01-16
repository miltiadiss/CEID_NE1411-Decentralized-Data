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

# Calculate mean for 'precipitation' column
mean_precipitation = bike_data.select(mean(col("precipitation"))).collect()[0][0]

# Handle None case for mean_precipitation
if mean_precipitation is None:
    mean_precipitation = 0.0  # Assign default value

# Ensure the value is of the correct type
mean_precipitation = float(mean_precipitation)

# Replace NULL values with the mean
bike_data = bike_data.fillna({"precipitation": mean_precipitation})

# Data Preprocessing
for column in ['temperature', 'wind_speed', 'precipitation', 'cloudiness']:
    mean_value = bike_data.select(mean(col(column))).collect()[0][0]
    bike_data = bike_data.fillna({column: mean_value})

bike_data = bike_data.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# Clean data by removing rows with NaN values in critical columns
bike_data_cleaned = bike_data.dropna(subset=["timestamp", "temperature", "wind_speed", "precipitation", "cloudiness"])

# Feature Engineering for Time
bike_data_cleaned = bike_data_cleaned.withColumn("hour_of_day", hour(col("timestamp")))
bike_data_cleaned = bike_data_cleaned.withColumn("day_of_week", dayofweek(col("timestamp")))
bike_data_cleaned = bike_data_cleaned.withColumn("is_weekend", when((col("day_of_week") == 1) | (col("day_of_week") == 7), 1).otherwise(0))

# Define a window partitioned by city_name and ordered by timestamp
window_spec = Window.partitionBy("city_name").orderBy("timestamp")

# Add lagged features
bike_data_cleaned = bike_data_cleaned.withColumn(
    "average_docking_station_utilisation_lag1",
    lag("average_docking_station_utilisation", 1).over(window_spec)
)
bike_data_cleaned = bike_data_cleaned.withColumn(
    "average_docking_station_utilisation_lag2",
    lag("average_docking_station_utilisation", 2).over(window_spec)
)

# Drop rows with null lagged values (e.g., first few rows in each partition)
bike_data_cleaned = bike_data_cleaned.dropna(
    subset=["average_docking_station_utilisation_lag1", "average_docking_station_utilisation_lag2"]
)

# Feature Scaling
feature_columns = [
    'temperature', 'wind_speed', 'precipitation', 'cloudiness',
    'hour_of_day', 'day_of_week', 'is_weekend',
    'average_docking_station_utilisation_lag1',
    'average_docking_station_utilisation_lag2'
]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data = assembler.transform(bike_data_cleaned)

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
    numTrees=100,
    maxDepth=10,
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

# 1. Feature Distribution (Histograms for all features in the same window using subplots)
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
sns.histplot(bike_data_cleaned.select("precipitation").toPandas(), kde=False, ax=axes[1, 0])
axes[1, 0].set_title("Distribution of Precipitation")
axes[1, 0].set_xlabel("Precipitation")
axes[1, 0].set_ylabel("Frequency")

# Plot for cloudiness
sns.histplot(bike_data_cleaned.select("cloudiness").toPandas(), kde=True, ax=axes[1, 1])
axes[1, 1].set_title("Distribution of Cloudiness")
axes[1, 1].set_xlabel("Cloudiness")
axes[1, 1].set_ylabel("Frequency")

plt.tight_layout()  # Adjust layout for better spacing
plt.show()

# 2. Correlation Heatmap
pandas_df = bike_data_cleaned.select("temperature", "wind_speed", "precipitation", "cloudiness", "hour_of_day", "day_of_week", "is_weekend", "average_docking_station_utilisation").toPandas()
plt.figure(figsize=(10, 6))
sns.heatmap(pandas_df.corr(), annot=True, cmap="coolwarm", fmt=".2f", linewidths=0.5)
plt.title("Correlation Heatmap")
plt.show()

# Get the last row and prepare next hour input
last_row = bike_data_cleaned.orderBy("timestamp", ascending=False).limit(1).collect()[0]
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

# Add time-based features for the next hour data
next_hour_data = next_hour_data.withColumn("hour_of_day", hour(col("timestamp")))
next_hour_data = next_hour_data.withColumn("day_of_week", dayofweek(col("timestamp")))
next_hour_data = next_hour_data.withColumn("is_weekend", when((col("day_of_week") == 1) | (col("day_of_week") == 7), 1).otherwise(0))

# Add lagged features for the next hour prediction
next_hour_data = next_hour_data.withColumn(
    "average_docking_station_utilisation_lag1",
    lit(last_row.average_docking_station_utilisation)
)
next_hour_data = next_hour_data.withColumn(
    "average_docking_station_utilisation_lag2",
    lit(last_row.average_docking_station_utilisation_lag1)
)

# Prepare features for prediction
next_hour_features = assembler.transform(next_hour_data)

# Scale the features for the next hour
next_hour_scaled = scaler_model.transform(next_hour_features)

# Predict for next hour
next_hour_prediction = rf_model.transform(next_hour_scaled)

# Extract and display the prediction
next_hour_result = next_hour_prediction.select("prediction").collect()
print(f"Predicted utilization for the next hour ({next_timestamp}) is: {next_hour_result[0]['prediction']}")
