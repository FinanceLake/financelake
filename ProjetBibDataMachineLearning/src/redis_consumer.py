# Consommateur Redis - Reçoit les prix et prédit avec LSTM
import redis
import json
import pickle
import numpy as np
from datetime import datetime
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))

from src.model import LSTMModel
from config import Config

class RedisConsumer:
    def __init__(self, host='localhost', port=6379, db=0):
        """Initialise le consommateur Redis"""
        self.redis_client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self.channel = 'amzn_prices'
        self.config = Config
        self.model = None
        self.scaler = None
        self.price_history = []
        self.predictions = []
        self.lookback = self.config.LOOKBACK
        
        self.test_connection()
        self.load_model_and_scaler()
        
    def test_connection(self):
        """Vérifie la connexion à Redis"""
        try:
            self.redis_client.ping()
            print("✓ Connexion Redis établie")
        except Exception as e:
            print(f"✗ Erreur de connexion Redis: {e}")
            raise
    
    def load_model_and_scaler(self):
        """Charge le modèle LSTM et le scaler"""
        try:
            # Charger le modèle
            model_path = self.config.GOLD_DIR / self.config.MODEL_FILE
            if model_path.exists():
                loaded = LSTMModel.load_model(str(model_path))
                if loaded is None:
                    print(f"✗ Impossible de charger le modèle: {model_path}")
                    return False
                self.model = loaded
                print(f"✓ Modèle LSTM chargé: {model_path}")
            else:
                print(f"✗ Modèle non trouvé: {model_path}")
                print("  Assurez-vous d'avoir entraîné le modèle d'abord")
                return False
            
            # Charger le scaler
            scalers_path = self.config.GOLD_DIR / 'scalers.pkl'
            if scalers_path.exists():
                with open(scalers_path, 'rb') as f:
                    scalers = pickle.load(f)
                # On utilise le scaler_y pour normaliser/dénormaliser les prix
                self.scaler = scalers.get('scaler_y')
                if self.scaler is None:
                    print(f"✗ Le fichier {scalers_path} ne contient pas 'scaler_y'")
                    return False
                print(f"✓ Scalers chargés: {scalers_path}")
            else:
                print(f"✗ Scalers non trouvés: {scalers_path}")
                return False
            
            return True
        except Exception as e:
            print(f"✗ Erreur lors du chargement: {e}")
            return False
    
    def normalize_price(self, price):
        """Normalise le prix avec le scaler"""
        if self.scaler is None:
            return price
        return self.scaler.transform([[price]])[0][0]
    
    def denormalize_price(self, normalized_price):
        """Dénormalise le prix"""
        if self.scaler is None:
            return normalized_price
        return self.scaler.inverse_transform([[normalized_price]])[0][0]
    
    def predict_next_price(self, prices):
        """Prédit le prochain prix avec le modèle LSTM"""
        if self.model is None or len(prices) < self.lookback:
            return None
        
        try:
            # Prendre les derniers lookback prix
            recent_prices = np.array(prices[-self.lookback:])
            
            # Normaliser
            normalized = self.scaler.transform(recent_prices.reshape(-1, 1)).flatten()
            
            # Pour le LSTM, on a besoin de 20 features (voir pipeline.py)
            # Créer les features MA et autres basées sur les prix
            features = []
            for i in range(len(normalized)):
                price = normalized[i]
                # Ajouter des lags simples
                lag1 = normalized[i-1] if i > 0 else price
                lag2 = normalized[i-2] if i > 1 else price
                # Simple Moving Averages
                ma5 = np.mean(normalized[max(0, i-4):i+1])
                ma10 = np.mean(normalized[max(0, i-9):i+1])
                
                # Créer un vecteur de features (20 colonnes)
                feature_row = [price, lag1, lag2, ma5, ma10] + [price] * 15
                features.append(feature_row[:20])  # Garder 20 features
            
            X = np.array(features).reshape(1, self.lookback, 20)
            
            # Prédiction
            normalized_prediction = self.model.predict(X, verbose=0)[0][0]
            
            # Dénormaliser
            predicted_price = self.denormalize_price(normalized_prediction)
            
            return round(float(predicted_price), 2)
        except Exception as e:
            print(f"✗ Erreur de prédiction: {e}")
            return None
    
    def consume_messages(self, max_messages=None):
        """Consomme les messages Redis et prédit"""
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe(self.channel)
        
        message_count = 0
        
        print(f"\n{'='*70}")
        print(f"CONSOMMATEUR REDIS - PRÉDICTION EN TEMPS RÉEL")
        print(f"{'='*70}")
        print(f"Canal écouté: {self.channel}")
        print(f"Lookback: {self.lookback} prix")
        if max_messages:
            print(f"Messages max: {max_messages}")
        print(f"{'='*70}\n")
        
        try:
            for message in pubsub.listen():
                if message['type'] == 'message':
                    message_count += 1
                    
                    # Parser le message
                    data = json.loads(message['data'])
                    current_price = data['price']
                    timestamp = data['timestamp']
                    
                    # Ajouter à l'historique
                    self.price_history.append(current_price)
                    
                    # Prédire si on a assez de données
                    prediction = self.predict_next_price(self.price_history)
                    
                    # Afficher le résultat
                    if prediction:
                        change = prediction - current_price
                        percent_change = (change / current_price) * 100
                        direction = "📈" if change > 0 else "📉"
                        
                        print(f"[{message_count:04d}] {timestamp}")
                        print(f"  Prix actuel:  ${current_price:.2f}")
                        print(f"  Prédiction:   ${prediction:.2f} {direction} ({change:+.2f} | {percent_change:+.2f}%)")
                        print()
                        
                        # Sauvegarder la prédiction
                        self.predictions.append({
                            'timestamp': timestamp,
                            'current_price': current_price,
                            'predicted_price': prediction,
                            'change': change,
                            'percent_change': percent_change
                        })
                    else:
                        print(f"[{message_count:04d}] {timestamp} | Prix: ${current_price:.2f} (Accumulant les données...)")
                    
                    # Vérifier la limite de messages
                    if max_messages and message_count >= max_messages:
                        print(f"\n✓ Consommation terminée ({message_count} messages)")
                        break
                        
        except KeyboardInterrupt:
            print(f"\n\n✓ Consommateur arrêté après {message_count} messages")
        except Exception as e:
            print(f"\n✗ Erreur: {e}")
        finally:
            pubsub.close()
            self.save_predictions()
    
    def save_predictions(self):
        """Sauvegarde les prédictions"""
        if self.predictions:
            output_path = self.config.GOLD_DIR / 'redis_predictions.json'
            with open(output_path, 'w') as f:
                json.dump(self.predictions, f, indent=2)
            print(f"\n✓ Prédictions sauvegardées: {output_path}")
            print(f"  Total: {len(self.predictions)} prédictions")


if __name__ == "__main__":
    consumer = RedisConsumer()
    
    # Consommer 10 messages
    consumer.consume_messages(max_messages=10)
