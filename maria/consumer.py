from confluent_kafka import Consumer
import json

# Kafka Consumer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'  # Start reading from the earliest message
}

consumer = Consumer(conf)
consumer.subscribe(['weather_topic', 'station_info_topic', 'station_status_topic'])

# Processing logic
def process_message(topic, message):
    if topic == 'weather_topic':
        print(f"Processing weather data: {message}")
        # Add logic to process and store weather data
    elif topic == 'station_info_topic':
        print(f"Processing station info: {message}")
        # Add logic to process and store station info
    elif topic == 'station_status_topic':
        print(f"Processing station status: {message}")
        # Add logic to process and store station status
    else:
        print(f"Unknown topic: {topic}, message: {message}")

# Consuming messages
print("Starting consumer...")
try:
    while True:
        msg = consumer.poll(1.0)  # Poll for a message
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        topic = msg.topic()
        message = json.loads(msg.value().decode('utf-8'))  # Deserialize message
        process_message(topic, message)

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()
