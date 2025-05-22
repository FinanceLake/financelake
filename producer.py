from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {"symbol": "AAPL", "price": 187.21, "timestamp": time.time()}
    producer.send("finance-topic", value=data)
    print(f"Sent: {data}")
    time.sleep(5)
