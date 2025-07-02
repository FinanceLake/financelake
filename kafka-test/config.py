from dotenv import load_dotenv
import os

# Charge automatiquement le fichier .env à la racine du projet
load_dotenv()

# Récupération des variables d'environnement
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

# Tu peux ajouter toutes les variables utiles ici

if __name__ == "__main__":
    # Test rapide pour vérifier que ça marche
    print(f"KAFKA_BROKER = {KAFKA_BROKER}")
    print(f"KAFKA_TOPIC = {KAFKA_TOPIC}")
