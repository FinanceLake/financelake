from kafka import KafkaProducer
import json

# Connexion au broker Kafka local
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Message à envoyer
message = {"symbol": "AAPL", "price": 183.45}

# Envoi dans le topic "test-topic"
producer.send('test-topic', message)
producer.flush()

print("✅ Message envoyé avec succès !")
