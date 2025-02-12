import logging
import json
import time
from typing import Dict, Any, Optional
from confluent_kafka import Producer
import requests
import jsonschema
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        #logs to a file
        logging.FileHandler('kafka_producer.log'), 
        #logs to the console
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# JSON Schemas for data validation
# Schema for validating weather data
WEATHER_SCHEMA = {
    "type": "object",
    "required": ["main", "weather", "wind"],
    "properties": {
        "main": {
            "type": "object",
            "required": ["temp", "humidity"],
            "properties": {
                "temp": {"type": "number"},
                "humidity": {"type": "number"}
            }
        },
        "weather": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["description"],
                "properties": {
                    "description": {"type": "string"}
                }
            }
        },
        "wind": {
            "type": "object", 
            "required": ["speed"],
            "properties": {
                "speed": {"type": "number"}
            }
        }
    }
}

# Schema for validating station information data
STATION_INFO_SCHEMA = {
    "type": "object",
    "required": ["data", "last_updated"],
    "properties": {
        "data": {
            "type": "object",
            "required": ["stations"],
            "properties": {
                "stations": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "required": ["station_id", "name", "lat", "lon"],
                        "properties": {
                            "station_id": {"type": "string"},
                            "name": {"type": "string"},
                            "lat": {"type": "number"},
                            "lon": {"type": "number"}
                        }
                    }
                }
            }
        },
        "last_updated": {"type": "number"}
    }
}
# Schema for validating station status data
STATION_STATUS_SCHEMA = {
    "type": "object",
    "required": ["data", "last_updated"],
    "properties": {
        "data": {
            "type": "object",
            "required": ["stations"],
            "properties": {
                "stations": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "required": ["station_id", "num_bikes_available", "num_docks_available"],
                        "properties": {
                            "station_id": {"type": "string"},
                            "num_bikes_available": {"type": "number"},
                            "num_docks_available": {"type": "number"}
                        }
                    }
                }
            }
        },
        "last_updated": {"type": "number"}
    }
}

# Kafka setup
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

# URLs for weather data, station information, and station status
weather_url = "https://api.openweathermap.org/data/2.5/weather?lat=25.276987&lon=55.296249&appid=26d2a4e587fc68ba1d98399638a19231"
station_info_url = "https://dubai.publicbikesystem.net/customer/gbfs/v2/en/station_information"
station_status_url = "https://dubai.publicbikesystem.net/customer/gbfs/v2/en/station_status"

#Validate JSON data against a given schema.
def validate_data(data: Dict[str, Any], schema: Dict[str, Any]) -> bool:
    try:
        jsonschema.validate(instance=data, schema=schema)  
        return True
    except jsonschema.exceptions.ValidationError as e:
        logger.error(f"Data validation error: {e}")
        return False

#Fetch data from an API with rate limiting, retries, and error handling.  
def fetch_data(url: str, max_retries: int = 5) -> Optional[Dict[str, Any]]:
    retries = 0
    backoff = 1  # Start with a 1-second backoff
    while retries < max_retries:
        try:
            response = requests.get(url, timeout=10) # Make a GET request
            response.raise_for_status()  
            
            data = response.json() # Parse the JSON response
            
            # Validate data based on URL
            if 'weather' in url:
                if validate_data(data, WEATHER_SCHEMA):
                    return data
            elif 'station_information' in url:
                if validate_data(data, STATION_INFO_SCHEMA):
                    return data
            elif 'station_status' in url:
                if validate_data(data, STATION_STATUS_SCHEMA):
                    return data
            
            logger.warning(f"Data validation failed for {url}")
            return None
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for {url}: {e}")
            time.sleep(backoff) # Wait before retrying
            backoff *= 2  # Exponential backoff
            retries += 1
        except json.JSONDecodeError as e:
            logger.error(f"JSON decoding error for {url}: {e}")
            return None
    
    logger.error(f"Failed to fetch data from {url} after {max_retries} retries.")
    return None


# Kafka message delivery callback with console and file logging.
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery FAILED: {err}")
        logger.error(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()}")
        logger.info(f"Message delivered to {msg.topic()}")

#Continuously fetch and produce data to Kafka topics with error handling.
def produce_data():
    print("Starting Kafka Producer...")

    # Initialize to 0 to ensure immediate fetching
    last_weather_update = 0  
    last_station_update = 0  
    
    try:
        while True:
            print("\n--- Fetching Data ---")
            current_time = time.time()
            
            # Fetch Weather API every 1 hour or first run
            if current_time - last_weather_update >= 3600 or last_weather_update == 0:  
                weather_data = fetch_data(weather_url)
                if weather_data:
                    print("Weather data fetched successfully")
                    producer.produce('weather_topic', key='weather', value=json.dumps(weather_data), callback=delivery_report)
                    last_weather_update = current_time  # Update the last fetch time

            # Fetch Station Info and Status API every 5 minutes or first run
            if current_time - last_station_update >= 300 or last_station_update == 0: 
                station_info_data = fetch_data(station_info_url)
                if station_info_data:
                    print("Station Info data fetched successfully")
                    producer.produce('station_info_topic', key='station_info', value=json.dumps(station_info_data), callback=delivery_report)

                station_status_data = fetch_data(station_status_url)
                if station_status_data:
                    print("Station Status data fetched successfully")
                    producer.produce('station_status_topic', key='station_status', value=json.dumps(station_status_data), callback=delivery_report)

                last_station_update = current_time  # Update the last fetch time
            
            producer.flush()  # Ensure all messages are sent
            print(f"Waiting for next fetch cycle...")
            time.sleep(30)  # Sleep for 30 seconds before checking again
    
    except Exception as e:
        print(f"Unhandled exception: {e}")
        logger.critical(f"Unhandled exception in produce_data: {e}")
    finally:
        producer.flush()  # Ensure no messages are left in the buffer
        producer.close()  # Close the producer

if __name__ == "__main__":
    try:
        print("Kafka Producer is starting...")
        produce_data()  # Start producing data
    except KeyboardInterrupt:
        print("\nKafka Producer stopped by user")
        logger.info("Kafka Producer stopped by user")
