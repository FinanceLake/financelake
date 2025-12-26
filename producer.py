from kafka import KafkaProducer
import json
import time
from config import KAFKA_BROKER, KAFKA_TOPIC

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Producing messages to topic '{KAFKA_TOPIC}' at broker '{KAFKA_BROKER}'")

# Send data in a loop
while True:
    data = {
        "symbol": "AAPL",
        "price": 187.21,
        "timestamp": time.time()
    }
    producer.send(KAFKA_TOPIC, value=data)
    print(f"Sent: {data}")
    time.sleep(5)
