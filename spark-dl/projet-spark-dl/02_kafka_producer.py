# 02_kafka_producer.py
from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']
current_prices = {
    'AAPL': 150.0, 'GOOGL': 2800.0, 'MSFT': 330.0,
    'TSLA': 200.0, 'AMZN': 3500.0
}

print("🚀 Producteur Kafka démarré")
print("📤 Envoi vers le topic: stock-prices")
print("⏱️  Fréquence: 1 message/seconde")
print("-" * 50)

def generate_data():
    """Génère des données boursières réalistes"""
    symbol = random.choice(symbols)
    current = current_prices[symbol]
    
    # Variation basée sur l'heure
    hour = datetime.now().hour
    if 9 <= hour < 12:  # Matin haussier
        variation = random.uniform(-0.005, 0.015)
    elif 12 <= hour < 14:  # Mid-day
        variation = random.uniform(-0.01, 0.01)
    else:  # Après-midi volatile
        variation = random.uniform(-0.02, 0.02)
    
    new_price = current * (1 + variation)
    current_prices[symbol] = new_price
    
    # Volume corrélé avec la variation
    base_volume = 10000
    volume = int(base_volume * (1 + abs(variation) * 10))
    
    return {
        'symbol': symbol,
        'price': round(new_price, 2),
        'volume': volume,
        'timestamp': datetime.now().isoformat(),
        'hour': hour,
        'minute': datetime.now().minute,
        'price_change': round(variation * 100, 4)
    }

try:
    count = 0
    while True:
        data = generate_data()
        
        # Envoyer à Kafka
        producer.send('stock-prices', data)
        
        count += 1
        if count % 10 == 0:
            print(f"📨 {count} | {data['symbol']}: ${data['price']} "
                  f"({data['price_change']:+.2f}%)")
        
        time.sleep(1)
        
except KeyboardInterrupt:
    print("\n⏹️ Producteur arrêté")
finally:
    producer.close()
