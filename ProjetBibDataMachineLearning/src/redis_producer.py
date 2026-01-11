# Producteur Redis - Génère les prix en temps réel
import redis
import json
import time
import numpy as np
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path

class RedisProducer:
    def __init__(self, host='localhost', port=6379, db=0):
        """Initialise la connexion Redis"""
        self.redis_client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self.channel = 'amzn_prices'
        self.test_connection()
        
    def test_connection(self):
        """Vérifie la connexion à Redis"""
        try:
            self.redis_client.ping()
            print("✓ Connexion Redis établie")
        except Exception as e:
            print(f"✗ Erreur de connexion Redis: {e}")
            raise
    
    def load_historical_data(self):
        """Charge les données historiques pour le contexte"""
        bronze_path = Path(__file__).parent.parent / "data" / "bronze" / "AMZN_historical.csv"
        if bronze_path.exists():
            df = pd.read_csv(bronze_path)
            df['Date'] = pd.to_datetime(df['Date'])
            return df.sort_values('Date')
        return None
    
    def generate_realistic_price(self, last_price, volatility=0.02):
        """
        Génère un prix réaliste basé sur le dernier prix
        volatility: écart-type du changement quotidien
        """
        # Mouvement brownien géométrique
        daily_return = np.random.normal(0, volatility)
        new_price = last_price * (1 + daily_return)
        return round(new_price, 2)
    
    def stream_prices(self, initial_price=150.0, interval=5, duration=None):
        """
        Diffuse les prix en temps réel vers Redis
        
        Args:
            initial_price: Prix de départ
            interval: Intervalle entre les messages (secondes)
            duration: Durée totale (secondes), None = infini
        """
        current_price = initial_price
        start_time = time.time()
        message_count = 0
        
        print(f"\n{'='*60}")
        print(f"PRODUCTEUR REDIS - STREAMING PRIX AMZN")
        print(f"{'='*60}")
        print(f"Canal: {self.channel}")
        print(f"Prix initial: ${current_price:.2f}")
        print(f"Intervalle: {interval}s")
        if duration:
            print(f"Durée: {duration}s")
        print(f"{'='*60}\n")
        
        try:
            while True:
                # Vérifier la durée
                elapsed_time = time.time() - start_time
                if duration and elapsed_time > duration:
                    print(f"\n✓ Streaming terminé après {message_count} messages")
                    break
                
                # Générer le nouveau prix
                current_price = self.generate_realistic_price(current_price)
                
                # Créer le message
                message = {
                    'timestamp': datetime.now().isoformat(),
                    'price': current_price,
                    'symbol': 'AMZN',
                    'message_id': message_count
                }
                
                # Publier sur Redis
                self.redis_client.publish(self.channel, json.dumps(message))
                
                # Afficher le message
                print(f"[{message_count:04d}] {message['timestamp']} | Prix: ${current_price:.2f}")
                message_count += 1
                
                # Attendre avant le prochain message
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print(f"\n\n✓ Arrêt du producteur après {message_count} messages")
        except Exception as e:
            print(f"\n✗ Erreur: {e}")
    
    def stream_batch(self, num_messages=10, interval=2):
        """Diffuse un nombre défini de messages"""
        self.stream_prices(duration=num_messages * interval)


if __name__ == "__main__":
    producer = RedisProducer()
    
    # Commencer le streaming (10 messages, 2 secondes d'intervalle)
    producer.stream_batch(num_messages=10, interval=2)
