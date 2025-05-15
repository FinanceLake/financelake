from kafka import KafkaProducer
import json, time, random

# Création du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Envoi continu de données simulées
while True:
    data = {
        "symbol": "AAPL",
        "price": round(random.uniform(170, 180), 2),
        "timestamp": time.time()
    }
    print("Envoi :", data)
    producer.send('finance', data)  
    time.sleep(2)
