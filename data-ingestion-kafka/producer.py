import csv
import json
import time
from kafka import KafkaProducer
from config import KAFKA_TOPIC, KAFKA_SERVER, CSV_FILE

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

try:
    with open(CSV_FILE, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            producer.send(KAFKA_TOPIC, value=row)
            print(f"Sent: {row}")
            time.sleep(0.5)
    producer.flush()
except Exception as e:
    print("Error producing to Kafka:", e)
