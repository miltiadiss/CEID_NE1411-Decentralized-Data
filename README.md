# Overview
This project is part of **Decentralized Data Technologies** elective course in Computer Engineering & Informatics Department of University of Patras for Winter Semester 2024-2025 (Semester 9).

This project involves the implementation of a real-time bike-sharing analytics system using decentralized data technologies. It integrates live data from bike-sharing systems and weather APIs, processes it using **Apache Kafka** for streaming and **Apache Spark** for distributed processing and builds a **machine learning model** to predict bike utilization. The system performs tasks such as data ingestion, real-time analysis, utilization rate calculations, and next-hour predictions while adhering to scalable and efficient software engineering practices.

![image](https://github.com/user-attachments/assets/b909ec1f-3d32-4db2-b6b6-0c302ac3b3ef)

## Pipeline
### 1. Data Ingestion
- **Fetch Live Data**:
  - Bike-sharing data (station information and status).
  - Weather data (temperature, precipitation, wind speed, and cloudiness).
- **Validation**:
  - Handle missing values, API rate limits, and errors.
  - Transform timestamps into usable features (`hour_of_day`, `day_of_week`).
- **Stream Data**:
  - Use Apache Kafka to stream data into separate topics for station info, station status, and weather.

### 2. Data Processing
- **Consume Data**:
  - Read data from Kafka topics into Apache Spark.
- **Analytics**:
  - Join bike-sharing and weather data.
  - Calculate utilization rates (average, max, min, standard deviation).
  - Correlate utilization with weather conditions.
  - Generate hourly usage summaries.
- **Storage**:
  - Save processed data into a csv file for ease.

### 3. Machine Learning
- **Training Data**:
  - Collect and store data over a period of 6 days.
- **Regression Model**:
  - Train The model using SparkML with features such as time and weather metrics.
  - Predict bike utilization for the next hour.
- **Evaluation**:
  - Assess model performance using metrics like RMSE, MAE, and R².

### 4. Visualization and Insights
- Create visualizations for:
  - Utilization patterns over time.
  - Weather correlations with bike usage.
  - Model performance metrics.

### 5. User Input and Prediction
- Accept user inputs for weather and time to predict next-hour utilization.
- Provide actionable insights for operational improvements.

## Data Creation
1. **Data Sources**:
   - **Bike-Sharing Data** (via GBFS API):
     - **Station Information**: Static data such as station names, locations, capacities.
     - **Station Status**: Real-time data on bike availability and dock usage.
   - **Weather Data** (via OpenWeatherMap API):
     - **Weather Information**: temperature, precipitation, wind speed, cloudiness.
   - Data is also collected at regular intervals.

2. **Extra Features Generated**:
   - **Time-Based Features for ML**: hour_of_day, day_of_week, is_weekend. These are used for **Feature Engineering** during the ML Model training phase.
   - **Docking Station Metrics**: average, max, min, and standard deviation of utilization rates of docking stations.

## Goals
1. **Real-Time Analytics**:
   - Calculate utilization rates for docking stations and cities in real-time.
   - Correlate weather conditions with bike utilization.
2. **Machine Learning**:
   - Predict on-demand bike utilization for the next hour using a regression model. We chose a **Random Forest Regressor**.
3. **Scalability**:
   - Handle increasing data volumes efficiently with distributed computing frameworks.

## Programming Environment and Tools
Python, Linux (Ubuntu or Debian 12.8)
