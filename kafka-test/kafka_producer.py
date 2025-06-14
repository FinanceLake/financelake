from kafka import KafkaProducer
import json
import time
from config import KAFKA_BROKER, KAFKA_TOPIC

# Connexion au broker Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Envoi d’un message unique (comme dans ta version)
message = {"symbol": "AAPL", "price": 183.45, "timestamp": time.time()}
producer.send(KAFKA_TOPIC, message)
producer.flush()

print(f"✅ Message envoyé avec succès à {KAFKA_TOPIC} sur {KAFKA_BROKER}")
