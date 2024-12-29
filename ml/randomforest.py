from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, FloatType, TimestampType, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("Bike Utilization Prediction").getOrCreate()

# Load the dataset
file_path = '/home/unix/ml/dataset.csv'
bike_data = spark.read.csv(file_path, header=True, inferSchema=True)

# Select relevant features and target variable
feature_columns = ['temperature', 'wind_speed', 'precipitation', 'cloudiness']
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data = assembler.transform(bike_data).select("features", "average_docking_station_utilisation")

# Split the dataset
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

# Create and train the Random Forest Regressor
rf_regressor = RandomForestRegressor(featuresCol="features", 
                                   labelCol="average_docking_station_utilisation", 
                                   numTrees=100, 
                                   seed=42)
rf_model = rf_regressor.fit(train_data)

# Create next hour prediction data
next_hour_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("city_name", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("wind_speed", FloatType(), True),
    StructField("precipitation", FloatType(), True),
    StructField("cloudiness", FloatType(), True)
])

# Extract the last row's data
last_row = bike_data.orderBy("timestamp").tail(1)[0]
# Convert the timestamp to datetime using the full format including microseconds
current_timestamp = datetime.strptime(str(last_row.timestamp), '%Y-%m-%d %H:%M:%S.%f')
next_timestamp = current_timestamp + timedelta(hours=1)

# Create next hour data with slight temperature increase
next_hour_data = spark.createDataFrame([
    (
        next_timestamp,
        str(last_row.city_name),
        float(last_row.temperature + 1.0),  # Assume 1 degree increase
        float(last_row.wind_speed),
        float(last_row.precipitation) if last_row.precipitation is not None else None,
        float(last_row.cloudiness)
    )
], schema=next_hour_schema)

# Prepare features for prediction
next_hour_features = assembler.transform(next_hour_data)
next_hour_prediction = rf_model.transform(next_hour_features)

# Make predictions on test data
predictions = rf_model.transform(test_data)

# Evaluate the model
evaluator = RegressionEvaluator(
    labelCol="average_docking_station_utilisation",
    predictionCol="prediction",
    metricName="rmse"
)
rmse = evaluator.evaluate(predictions)

r2_evaluator = RegressionEvaluator(
    labelCol="average_docking_station_utilisation",
    predictionCol="prediction",
    metricName="r2"
)
r2 = r2_evaluator.evaluate(predictions)

# Convert predictions to pandas and combine with next hour prediction
test_predictions = predictions.select(
    "average_docking_station_utilisation",
    "prediction"
).toPandas()

next_hour_pdf = next_hour_prediction.select(
    "timestamp",
    "temperature",
    "wind_speed",
    "precipitation",
    "cloudiness",
    "prediction"
).toPandas()

# Save predictions to CSV
output_path = "/home/unix/ml/prediction.csv"
test_predictions.to_csv(output_path, index=False)

next_hour_output_path = "/home/unix/ml/next_hour_prediction.csv"
next_hour_pdf.to_csv(next_hour_output_path, index=False)

# Print the results
print(f"Root Mean Squared Error (RMSE): {rmse:.2f}")
print(f"R-squared: {r2:.2f}")
print(f"Test predictions saved to: {output_path}")
print(f"Next hour prediction saved to: {next_hour_output_path}")
print("\nNext hour prediction:")
print(next_hour_pdf[["timestamp", "prediction"]].to_string(index=False))

# Verify the files exist and have content
try:
    verification_df = pd.read_csv(output_path)
    next_hour_verification_df = pd.read_csv(next_hour_output_path)
    print(f"\nSuccessfully verified saves:")
    print(f"Test predictions file contains {len(verification_df)} rows")
    print(f"Next hour predictions file contains {len(next_hour_verification_df)} rows")
except Exception as e:
    print(f"Error verifying save: {str(e)}")

spark.stop()
