import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import TimeSeriesSplit
from datetime import timedelta

# Load dataset
data = pd.read_csv('your_dataset.csv', parse_dates=['timestamp'])

# 1. Handle non-numeric missing values in 'precipitation'
# Replace string ' NULL' or any other representation of missing values with actual NaN
data['precipitation'] = data['precipitation'].replace([' NULL', 'NaN', 'null', ''], np.nan)

# Convert 'precipitation' to numeric values, forcing errors to NaN
data['precipitation'] = pd.to_numeric(data['precipitation'], errors='coerce')

# Impute missing values for 'precipitation' using the median strategy
imputer = SimpleImputer(strategy='median')
data['precipitation'] = imputer.fit_transform(data[['precipitation']])

# 2. Feature Engineering
# Extract time-based features from the 'timestamp' column
data['hour'] = data['timestamp'].dt.hour
data['day_of_week'] = data['timestamp'].dt.dayofweek
data['month'] = data['timestamp'].dt.month
data['is_weekend'] = data['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)

# Create lag features (lag of 1 hour)
data['temperature_lag_1'] = data['temperature'].shift(1)
data['wind_speed_lag_1'] = data['wind_speed'].shift(1)
data['avg_docking_station_utilisation_lag_1'] = data['average_docking_station_utilisation'].shift(1)

# Drop rows with NaN values created by lag features (first row will have NaN)
data = data.dropna()

# 3. Encoding Categorical Variables
# One-Hot Encoding for 'city_name'
encoder = OneHotEncoder(drop='first', sparse=False)
city_encoded = pd.DataFrame(encoder.fit_transform(data[['city_name']]), columns=encoder.get_feature_names_out(['city_name']))

# Concatenate the encoded columns back to the original dataframe
data = pd.concat([data, city_encoded], axis=1)
data.drop('city_name', axis=1, inplace=True)

# 4. Scaling the Features
# Define the features and target
X = data.drop(columns=['timestamp', 'temperature'])  # Drop timestamp and target column
y = data['temperature']

# Split the data into training and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

# Apply scaling to numerical features
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# 5. Model: Random Forest Regressor
# Define the Random Forest Regressor model
rf = RandomForestRegressor(random_state=42)

# 6. Hyperparameter Tuning using GridSearchCV
param_grid = {
    'n_estimators': [100, 200],
    'max_depth': [10, 20, None],
    'min_samples_split': [2, 5],
    'min_samples_leaf': [1, 2],
    'bootstrap': [True, False]
}

# Perform GridSearchCV for hyperparameter tuning
grid_search = GridSearchCV(estimator=rf, param_grid=param_grid, cv=3, n_jobs=-1, verbose=2)
grid_search.fit(X_train_scaled, y_train)

# Get the best model from GridSearchCV
best_rf_model = grid_search.best_estimator_

# 7. Evaluate the Model
y_pred = best_rf_model.predict(X_test_scaled)

# Calculate Mean Absolute Error (MAE), Root Mean Squared Error (RMSE), and R²
mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)

print(f'Mean Absolute Error: {mae}')
print(f'Root Mean Squared Error: {rmse}')
print(f'R²: {r2}')

# 8. Plotting the Metrics (MAE, RMSE, R²)
metrics = {'MAE': mae, 'RMSE': rmse, 'R²': r2}
plt.figure(figsize=(8, 5))
plt.bar(metrics.keys(), metrics.values(), color=['blue', 'green', 'red'])
plt.title('Model Evaluation Metrics')
plt.ylabel('Score')
plt.show()

# 9. Feature Distribution (Histograms for all features in the same window using subplots)
fig, axes = plt.subplots(2, 2, figsize=(14, 10))  # 2x2 grid for subplots

# Plot for temperature
sns.histplot(data['temperature'], kde=True, ax=axes[0, 0])
axes[0, 0].set_title("Distribution of Temperature")
axes[0, 0].set_xlabel("Temperature")
axes[0, 0].set_ylabel("Frequency")

# Plot for wind_speed
sns.histplot(data['wind_speed'], kde=True, ax=axes[0, 1])
axes[0, 1].set_title("Distribution of Wind Speed")
axes[0, 1].set_xlabel("Wind Speed")
axes[0, 1].set_ylabel("Frequency")

# Plot for precipitation
sns.histplot(data['precipitation'], kde=True, ax=axes[1, 0])
axes[1, 0].set_title("Distribution of Precipitation")
axes[1, 0].set_xlabel("Precipitation")
axes[1, 0].set_ylabel("Frequency")

# Plot for cloudiness
sns.histplot(data['cloudiness'], kde=True, ax=axes[1, 1])
axes[1, 1].set_title("Distribution of Cloudiness")
axes[1, 1].set_xlabel("Cloudiness")
axes[1, 1].set_ylabel("Frequency")

plt.tight_layout()  # Adjust layout for better spacing
plt.show()

# 10. Correlation Heatmap
correlation_df = data[['temperature', 'wind_speed', 'precipitation', 'cloudiness', 'average_docking_station_utilisation']]
plt.figure(figsize=(10, 6))
sns.heatmap(correlation_df.corr(), annot=True, cmap="coolwarm", fmt=".2f", linewidths=0.5)
plt.title("Correlation Heatmap")
plt.show()

# 11. Get the last row and prepare next hour input
last_row = data.iloc[-1]
current_timestamp = last_row['timestamp']
next_timestamp = current_timestamp + timedelta(hours=1)

# Create next hour data with updated timestamp (example: assume 1 degree increase in temperature)
next_hour_data = pd.DataFrame([{
    'timestamp': next_timestamp,
    'city_name': last_row['city_name'],
    'temperature': last_row['temperature'] + 1.0,  # Example: assume 1 degree increase
    'wind_speed': last_row['wind_speed'],
    'precipitation': last_row['precipitation'] if pd.notna(last_row['precipitation']) else 0.0,
    'cloudiness': last_row['cloudiness']
}])

# Prepare features for prediction
next_hour_features = next_hour_data.drop(columns=['timestamp'])  # Drop timestamp as it's not a feature

# Scale the features for the next hour
next_hour_scaled = scaler.transform(next_hour_features)

# Predict for next hour
next_hour_prediction = best_rf_model.predict(next_hour_scaled)

# Display the next hour prediction
next_hour_result = next_hour_data.copy()
next_hour_result['predicted_temperature'] = next_hour_prediction
print(next_hour_result)
