# Script pour streaming INFINI en temps réel
import sys
import os
import threading
import time

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.redis_producer import RedisProducer
from src.redis_consumer import RedisConsumer

def run_producer_infinite():
    """Lance le producteur en continu"""
    try:
        producer = RedisProducer()
        producer.stream_prices(initial_price=150.0, interval=2, duration=None)
    except KeyboardInterrupt:
        print("\n✓ Producteur arrêté")
    except Exception as e:
        print(f"✗ Erreur producteur: {e}")

def run_consumer_infinite():
    """Lance le consommateur en continu"""
    try:
        consumer = RedisConsumer()
        consumer.consume_messages(max_messages=None)  # Pas de limite
    except KeyboardInterrupt:
        print("\n✓ Consommateur arrêté")
    except Exception as e:
        print(f"✗ Erreur consommateur: {e}")

def main():
    """Lance le système en streaming continu"""
    print("=" * 70)
    print("STREAMING INFINI EN TEMPS RÉEL - APPUYEZ SUR CTRL+C POUR ARRÊTER")
    print("=" * 70)
    
    # Vérifier Redis
    print("\n[1/2] Vérification de Redis...")
    try:
        producer = RedisProducer()
        print("✓ Redis est opérationnel\n")
    except Exception as e:
        print(f"✗ Impossible de se connecter à Redis: {e}")
        print("Assurez-vous que Redis est démarré")
        return
    
    # Vérifier le modèle
    print("[2/2] Vérification du modèle LSTM...")
    try:
        consumer = RedisConsumer()
        if consumer.model is None:
            print("✗ Modèle LSTM non trouvé!")
            return
        print("✓ Modèle LSTM chargé\n")
    except Exception as e:
        print(f"✗ Erreur: {e}")
        return
    
    print("-" * 70)
    print("Démarrage du streaming continu...")
    print("-" * 70 + "\n")
    
    # Lancer le producteur dans un thread
    producer_thread = threading.Thread(
        target=run_producer_infinite,
        daemon=False
    )
    
    producer_thread.start()
    
    # Attendre un peu avant le consommateur
    time.sleep(1)
    
    # Lancer le consommateur
    try:
        run_consumer_infinite()
    except KeyboardInterrupt:
        print("\n\n✓ Système arrêté par l'utilisateur")
    
    # Attendre fin du producteur
    producer_thread.join(timeout=5)
    
    print("\n" + "=" * 70)
    print("Streaming terminé")
    print("=" * 70)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n✓ Arrêt")
