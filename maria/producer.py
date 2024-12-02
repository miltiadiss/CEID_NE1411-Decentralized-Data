from confluent_kafka import Producer
import requests
import json
import time

# Kafka setup
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

# API endpoints
weather_url = "https://api.openweathermap.org/data/2.5/weather?lat=25.276987&lon=55.296249&appid=26d2a4e587fc68ba1d98399638a19231"
station_info_url = "https://dubai.publicbikesystem.net/customer/gbfs/v2/en/station_information"
station_status_url = "https://dubai.publicbikesystem.net/customer/gbfs/v2/en/station_status"

# Function to fetch data from an API with rate limiting and retries
def fetch_data(url, max_retries=5):
    retries = 0
    backoff = 1  # Start with a 1-second backoff
    while retries < max_retries:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:  # Too Many Requests
                print("Rate limit exceeded. Retrying...")
                time.sleep(backoff)
                backoff *= 2  # Exponential backoff
            else:
                print(f"Error fetching data: {response.status_code}, {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}. Retrying...")
            time.sleep(backoff)
            backoff *= 2  # Exponential backoff
        retries += 1

    print(f"Failed to fetch data from {url} after {max_retries} retries.")
    return None

# Kafka delivery callback
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Produce data to Kafka
def produce_data():
    while True:
        # Weather API (every 30 mins)
        weather_data = fetch_data(weather_url)
        if weather_data:
            producer.produce('weather_topic', key='weather', value=json.dumps(weather_data), callback=delivery_report)

        # Station Info API (every 5 mins)
        station_info_data = fetch_data(station_info_url)
        if station_info_data:
            producer.produce('station_info_topic', key='station_info', value=json.dumps(station_info_data), callback=delivery_report)

        # Station Status API (every 5 mins)
        station_status_data = fetch_data(station_status_url)
        if station_status_data:
            producer.produce('station_status_topic', key='station_status', value=json.dumps(station_status_data), callback=delivery_report)

        producer.flush()  # Ensure all messages are sent
        time.sleep(5 * 60)  # Wait for 5 minutes

        # After every sixth 5-minute cycle, refresh weather data
        if time.time() % (30 * 60) < 5 * 60:
            weather_data = fetch_data(weather_url)
            if weather_data:
                producer.produce('weather_topic', key='weather', value=json.dumps(weather_data), callback=delivery_report)

produce_data()
