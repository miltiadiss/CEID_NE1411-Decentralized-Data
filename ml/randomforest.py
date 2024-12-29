from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import pandas as pd

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

# Make predictions
predictions = rf_model.transform(test_data)

# Convert predictions to pandas and save
# First, let's extract the feature values from the vector
pdf = predictions.select(
    "average_docking_station_utilisation",
    "prediction"
).toPandas()

# Save to CSV
output_path = "/home/unix/ml/prediction.csv"
pdf.to_csv(output_path, index=False)

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

# Print the results
print(f"Root Mean Squared Error (RMSE): {rmse:.2f}")
print(f"R-squared: {r2:.2f}")
print(f"Predictions saved to: {output_path}")

# Add verification
try:
    # Verify the file exists and has content
    verification_df = pd.read_csv(output_path)
    print(f"Successfully verified save: File contains {len(verification_df)} rows")
except Exception as e:
    print(f"Error verifying save: {str(e)}")

spark.stop()
