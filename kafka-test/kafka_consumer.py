from kafka import KafkaConsumer
import json

# Connexion au broker Kafka local
consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("🎧 En attente de messages...")

# Lire les messages
for message in consumer:
    print(f"📨 Message reçu: {message.value}")
