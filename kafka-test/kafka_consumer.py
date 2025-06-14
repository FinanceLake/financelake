from kafka import KafkaConsumer
import json
from config import KAFKA_BROKER, KAFKA_TOPIC

# Connexion au broker Kafka via les variables d'environnement
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='finance-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"🎧 Listening to topic '{KAFKA_TOPIC}' on broker '{KAFKA_BROKER}'...")

# Lire les messages
for message in consumer:
    print(f"📨 Message reçu: {message.value}")
