import os
from pathlib import Path
from datetime import datetime

class Config:
    # Chemins
    BASE_DIR = Path(__file__).parent
    DATA_DIR = BASE_DIR / "data"
    RAW_DATA_DIR = DATA_DIR / "raw"
    BRONZE_DIR = DATA_DIR / "bronze"
    SILVER_DIR = DATA_DIR / "silver"
    GOLD_DIR = DATA_DIR / "gold"
    
    # Fichiers
    CSV_FILE = "AMZN_historical.csv"
    MODEL_FILE = "lstm_amzn_model.h5"
    PREDICTIONS_FILE = "amzn_predictions.json"
    VISUALIZATION_FILE = "amzn_visualization.html"
    
    # Paramètres du modèle
    LOOKBACK = 60  # Nombre de jours pour les features
    TRAIN_TEST_SPLIT = 0.8
    EPOCHS = 50    # Réduit pour accélérer
    BATCH_SIZE = 32
    
    # Colonnes
    DATE_COL = "Date"
    PRICE_COL = "Close"
    
    @classmethod
    def create_directories(cls):
        """Crée tous les répertoires nécessaires"""
        dirs = [
            cls.RAW_DATA_DIR,
            cls.BRONZE_DIR,
            cls.SILVER_DIR,
            cls.GOLD_DIR
        ]
        for directory in dirs:
            directory.mkdir(parents=True, exist_ok=True)
        print("Répertoires créés avec succès")