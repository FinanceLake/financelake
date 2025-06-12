from kafka import KafkaProducer
import json
import time
import random

# Initialisation du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

# Liste des symboles simulés
symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

# Envoi de 30 messages
print("Démarrage de l'envoi des messages...")
for i in range(30):
    symbol = random.choice(symbols)
    price = round(random.uniform(100, 500), 2)
    data = {
        'symbol': symbol,
        'timestamp': int(time.time()),
        'price': price
    }
    producer.send('finance-multi-symbol', key=symbol, value=data)
    print(f"Sent: {data}")
    time.sleep(1)

producer.flush()
