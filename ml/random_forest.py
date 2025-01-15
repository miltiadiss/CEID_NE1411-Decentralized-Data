from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.types import StructType, StructField, FloatType, TimestampType, StringType, IntegerType
from pyspark.sql.functions import col, mean, to_timestamp
import matplotlib.pyplot as plt
import seaborn as sns
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
file_path = '/home/debian/spark-3.5.3-bin-hadoop3/saprk_data.csv'  # Path to the CSV file
bike_data = spark.read.csv(file_path, header=True, schema=schema)

# Data Preprocessing
# 1. Handle Missing Values
# Fill missing values with the mean of the respective columns
for column in ['temperature', 'wind_speed', 'precipitation', 'cloudiness']:
    mean_value = bike_data.select(mean(col(column))).collect()[0][0]
    bike_data = bike_data.fillna({column: mean_value})

# 2. Convert 'timestamp' column to TimestampType
bike_data = bike_data.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# 3. Feature Engineering
# For now, we'll use the existing features.

# 4. Feature Scaling
# We will scale the features (temperature, wind_speed, precipitation, cloudiness)
feature_columns = ['temperature', 'wind_speed', 'precipitation', 'cloudiness']
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data = assembler.transform(bike_data)

# Apply StandardScaler to scale the features
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
scaler_model = scaler.fit(data)
data = scaler_model.transform(data)

# Select the necessary columns
data = data.select("scaled_features", "average_docking_station_utilisation")

# Split dataset into training and testing data
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

# Train Random Forest Regressor model
rf_regressor = RandomForestRegressor(
    featuresCol="scaled_features",
    labelCol="average_docking_station_utilisation",
    numTrees=500,                # Start with 500 trees
    maxDepth=30,                 # Moderate depth to balance performance
    minInstancesPerNode=5,       # Ensure meaningful splits
    maxBins=32,                  # Default binning
    featureSubsetStrategy="auto", # Automatically select features
    subsamplingRate=0.8          # Use 80% of data for each tree
)
rf_model = rf_regressor.fit(train_data)

# Evaluate the model
train_predictions = rf_model.transform(train_data)

# Initialize evaluators
evaluator_rmse = RegressionEvaluator(
    labelCol="average_docking_station_utilisation",
    predictionCol="prediction",
    metricName="rmse"
)
evaluator_mae = RegressionEvaluator(
    labelCol="average_docking_station_utilisation",
    predictionCol="prediction",
    metricName="mae"
)
evaluator_r2 = RegressionEvaluator(
    labelCol="average_docking_station_utilisation",
    predictionCol="prediction",
    metricName="r2"
)

# Evaluate on training data
rmse_train = evaluator_rmse.evaluate(train_predictions)
mae_train = evaluator_mae.evaluate(train_predictions)
r2_train = evaluator_r2.evaluate(train_predictions)

print(f"Training Metrics:")
print(f"Root Mean Squared Error (RMSE): {rmse_train:.4f}")
print(f"Mean Absolute Error (MAE): {mae_train:.4f}")
print(f"R² (Coefficient of Determination): {r2_train:.4f}")

# 1. Feature Distribution (Histograms for all features in the same window using subplots)
fig, axes = plt.subplots(2, 2, figsize=(14, 10))  # 2x2 grid for subplots

# Plot for temperature
sns.histplot(bike_data.select("temperature").toPandas(), kde=True, ax=axes[0, 0])
axes[0, 0].set_title("Distribution of Temperature")
axes[0, 0].set_xlabel("Temperature")
axes[0, 0].set_ylabel("Frequency")

# Plot for wind_speed
sns.histplot(bike_data.select("wind_speed").toPandas(), kde=True, ax=axes[0, 1])
axes[0, 1].set_title("Distribution of Wind Speed")
axes[0, 1].set_xlabel("Wind Speed")
axes[0, 1].set_ylabel("Frequency")

# Plot for precipitation
sns.histplot(bike_data.select("precipitation").toPandas(), kde=True, ax=axes[1, 0])
axes[1, 0].set_title("Distribution of Precipitation")
axes[1, 0].set_xlabel("Precipitation")
axes[1, 0].set_ylabel("Frequency")

# Plot for cloudiness
sns.histplot(bike_data.select("cloudiness").toPandas(), kde=True, ax=axes[1, 1])
axes[1, 1].set_title("Distribution of Cloudiness")
axes[1, 1].set_xlabel("Cloudiness")
axes[1, 1].set_ylabel("Frequency")

plt.tight_layout()  # Adjust layout for better spacing
plt.show()

# 2. Correlation Heatmap
pandas_df = bike_data.select("temperature", "wind_speed", "precipitation", "cloudiness", "average_docking_station_utilisation").toPandas()
plt.figure(figsize=(10, 6))
sns.heatmap(pandas_df.corr(), annot=True, cmap="coolwarm", fmt=".2f", linewidths=0.5)
plt.title("Correlation Heatmap")
plt.show()

# Define schema for the next hour prediction
next_hour_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("city_name", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("wind_speed", FloatType(), True),
    StructField("precipitation", FloatType(), True),
    StructField("cloudiness", FloatType(), True),
    StructField("hour_of_day", IntegerType(), True)  # Include hour_of_day as a feature
])

# Get the last row and prepare next hour input
last_row = bike_data.orderBy("timestamp", ascending=False).limit(1).collect()[0]
current_timestamp = last_row['timestamp']
next_timestamp = current_timestamp + timedelta(hours=1)  # Increment by 1 hour

# Create next hour data with updated timestamp
next_hour_data = spark.createDataFrame([(
    next_timestamp,  # Updated timestamp for next hour
    last_row.city_name,
    float(last_row.temperature + 1.0),  # Example: assume 1 degree increase
    float(last_row.wind_speed),
    float(last_row.precipitation) if last_row.precipitation is not None else 0.0,
    float(last_row.cloudiness),
    int(next_timestamp.hour)  # Adding hour_of_day as feature for the next hour
)], schema=next_hour_schema)

# Prepare features for prediction
next_hour_features = assembler.transform(next_hour_data)

# Scale the features for the next hour
next_hour_scaled = scaler_model.transform(next_hour_features)

# Predict for next hour
next_hour_prediction = rf_model.transform(next_hour_scaled)

# Extract and display the prediction
next_hour_result = next_hour_prediction.select(
    "timestamp",  # This will now show the updated timestamp (next hour)
    "city_name",
    "temperature",
    "wind_speed",
    "precipitation",
    "cloudiness",
    "prediction"
)
next_hour_result.show()
