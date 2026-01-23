import json
import time
import yfinance as yf
from kafka import KafkaProducer
from datetime import datetime, timedelta
import os
import requests

class YahooFinanceProducer:
    def __init__(self):
        self.symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NFLX', 'NVDA']
        self.historical_data = {}
        self.current_index = {}
        self.web_app_url = os.getenv('WEB_APP_URL', 'http://localhost:5000')
        
        # Chargement des données historiques
        self.load_historical_data()
    
    def load_historical_data(self):
        """Charge les données historiques pour tous les symboles"""
        print("Chargement des données historiques depuis Yahoo Finance...")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)  # 30 jours de données
        
        for symbol in self.symbols:
            try:
                ticker = yf.Ticker(symbol)
                hist_data = ticker.history(start=start_date, end=end_date, interval="5m")
                
                if not hist_data.empty:
                    self.historical_data[symbol] = []
                    self.current_index[symbol] = 0
                    
                    for index, row in hist_data.iterrows():
                        stock_record = {
                            'symbol': symbol,
                            'price': round(float(row['Close']), 2),
                            'volume': int(row['Volume']),
                            'timestamp': index.isoformat(),
                            'open': round(float(row['Open']), 2),
                            'high': round(float(row['High']), 2),
                            'low': round(float(row['Low']), 2),
                            'close': round(float(row['Close']), 2),
                            'data_type': 'raw'  # Pour identifier les données brutes
                        }
                        self.historical_data[symbol].append(stock_record)
                    
                    print(f"✓ {symbol}: {len(self.historical_data[symbol])} enregistrements chargés")
                else:
                    print(f"✗ {symbol}: Aucune donnée disponible")
                    
            except Exception as e:
                print(f"Erreur lors du chargement de {symbol}: {e}")
    
    def get_next_data_point(self, symbol):
        """Récupère le prochain point de données pour un symbole"""
        if symbol not in self.historical_data or not self.historical_data[symbol]:
            return None
        
        current_idx = self.current_index[symbol]
        data_points = self.historical_data[symbol]
        
        if current_idx >= len(data_points):
            # Réinitialiser l'index pour rejouer les données
            self.current_index[symbol] = 0
            current_idx = 0
        
        data_point = data_points[current_idx].copy()
        # Mise à jour du timestamp pour le temps réel
        data_point['timestamp'] = datetime.now().isoformat()
        data_point['sent_to_web'] = False  # Flag pour suivre l'envoi web
        
        self.current_index[symbol] = current_idx + 1
        
        return data_point
    
    def send_to_web_app(self, data):
        """Envoie les données brutes à l'application web"""
        try:
            response = requests.post(
                f'{self.web_app_url}/api/raw-stock-data',
                json=data,
                headers={'Content-Type': 'application/json'},
                timeout=2
            )
            if response.status_code == 200:
                return True
            else:
                print(f"⚠️  Erreur envoi web raw data: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Erreur connexion API web pour données brutes: {e}")
            return False

def main():
    # Configuration Kafka depuis les variables d'environnement
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    topic = os.getenv('KAFKA_TOPIC', 'data_stock')
    
    print(f"Connexion à Kafka: {bootstrap_servers}")
    print(f"Topic: {topic}")
    
    # Initialisation du producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda v: v.encode('utf-8') if v else None,
            retries=5,
            request_timeout_ms=30000
        )
    except Exception as e:
        print(f"Erreur de connexion à Kafka: {e}")
        return
    
    yahoo_producer = YahooFinanceProducer()
    
    print("Démarrage de l'envoi des données boursières historiques en temps réel...")
    print("Appuyez sur Ctrl+C pour arrêter")
    
    try:
        while True:
            for symbol in yahoo_producer.symbols:
                stock_data = yahoo_producer.get_next_data_point(symbol)
                
                if stock_data:
                    # Envoi vers Kafka avec le symbole comme clé
                    producer.send(
                        topic=topic,
                        key=stock_data['symbol'],
                        value=stock_data
                    )
                    
                    # Envoi des données brutes à l'application web
                    web_success = yahoo_producer.send_to_web_app(stock_data)
                    
                    web_status = "✅" if web_success else "❌"
                    print(f"📤 {symbol}: ${stock_data['price']} (Volume: {stock_data['volume']}) {web_status} Web")
                else:
                    print(f"⚠️  Aucune donnée disponible pour {symbol}")
            
            # Pause de 2 secondes entre chaque lot pour simuler le temps réel
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\nArrêt du producteur...")
    except Exception as e:
        print(f"Erreur inattendue: {e}")
    finally:
        producer.close()
        print("Producteur Kafka fermé")

if __name__ == "__main__":
    main()