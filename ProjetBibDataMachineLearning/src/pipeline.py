# Pipeline de donn�es
import pandas as pd
import numpy as np
import json
import pickle
from datetime import datetime, timedelta
from pathlib import Path
import yfinance as yf
from sklearn.preprocessing import MinMaxScaler
import config

class DataPipeline:
    def __init__(self):
        self.config = config.Config
        self.config.create_directories()
        
    def download_data(self):
        """Télécharge les données AMZN depuis Yahoo Finance"""
        print("Téléchargement des données AMZN...")
        
        # Téléchargement des données
        amzn = yf.download('AMZN', start='2020-01-01', end=datetime.now().strftime('%Y-%m-%d'))
        amzn.reset_index(inplace=True)
        
        # Sauvegarde en bronze
        bronze_path = self.config.BRONZE_DIR / self.config.CSV_FILE
        amzn.to_csv(bronze_path, index=False)
        print(f"Données téléchargées et sauvegardées: {bronze_path}")
        
        return amzn
    
    def load_data(self):
        """Charge les données depuis le fichier CSV"""
        bronze_path = self.config.BRONZE_DIR / self.config.CSV_FILE
        if bronze_path.exists():
            df = pd.read_csv(bronze_path)
            df[self.config.DATE_COL] = pd.to_datetime(df[self.config.DATE_COL])
            # Convertir les colonnes numériques
            numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            return df
        else:
            return self.download_data()
    
    def create_features(self, df):
        """Crée les features pour le modèle LSTM"""
        print("Création des features...")
        
        # Tri par date
        df = df.sort_values(self.config.DATE_COL).reset_index(drop=True)
        
        # Features de lag
        for i in range(1, 11):
            df[f'lag_{i}'] = df[self.config.PRICE_COL].shift(i)
        
        # Retours
        df['returns'] = df[self.config.PRICE_COL].pct_change()
        
        # Moyennes mobiles
        df['ma_7'] = df[self.config.PRICE_COL].rolling(window=7).mean()
        df['ma_14'] = df[self.config.PRICE_COL].rolling(window=14).mean()
        df['ma_30'] = df[self.config.PRICE_COL].rolling(window=30).mean()
        
        # Écart-type mobile
        df['std_7'] = df[self.config.PRICE_COL].rolling(window=7).std()
        
        # Volume normalisé
        df['volume_norm'] = df['Volume'] / df['Volume'].rolling(window=20).mean()
        
        # Target (prix du jour suivant)
        df['target'] = df[self.config.PRICE_COL].shift(-1)
        
        # Suppression des NaN
        df_clean = df.dropna().reset_index(drop=True)
        
        # Sauvegarde en silver
        silver_path = self.config.SILVER_DIR / "amzn_features.csv"
        df_clean.to_csv(silver_path, index=False)
        print(f"Features sauvegardées: {silver_path}")
        
        return df_clean
    
    def prepare_lstm_data(self, df, lookback=60):
        """Prépare les données pour le modèle LSTM"""
        print("Préparation des données LSTM...")
        
        # Colonnes de features
        feature_cols = [col for col in df.columns if col not in [self.config.DATE_COL, 'target', 'Volume']]
        
        # Données d'entraînement
        X = df[feature_cols].values
        y = df['target'].values
        
        # Normalisation
        self.scaler_X = MinMaxScaler(feature_range=(0, 1))
        self.scaler_y = MinMaxScaler(feature_range=(0, 1))
        
        X_scaled = self.scaler_X.fit_transform(X)
        y_scaled = self.scaler_y.fit_transform(y.reshape(-1, 1))
        
        # Création des séquences
        X_seq, y_seq = [], []
        for i in range(lookback, len(X_scaled)-1):
            X_seq.append(X_scaled[i-lookback:i])
            y_seq.append(y_scaled[i])
        
        X_seq = np.array(X_seq)
        y_seq = np.array(y_seq)
        
        # Split train/test
        split_idx = int(len(X_seq) * self.config.TRAIN_TEST_SPLIT)
        
        X_train, X_test = X_seq[:split_idx], X_seq[split_idx:]
        y_train, y_test = y_seq[:split_idx], y_seq[split_idx:]
        
        print(f"Données préparées: Train={len(X_train)}, Test={len(X_test)}")
        
        return X_train, X_test, y_train, y_test
    
    def save_metadata(self, df):
        """Sauvegarde les métadonnées du pipeline"""
        metadata = {
            'last_update': datetime.now().isoformat(),
            'data_points': len(df),
            'date_range': {
                'start': df[self.config.DATE_COL].min().strftime('%Y-%m-%d'),
                'end': df[self.config.DATE_COL].max().strftime('%Y-%m-%d')
            },
            'features': list(df.columns),
            'config': {
                'lookback': self.config.LOOKBACK,
                'train_test_split': self.config.TRAIN_TEST_SPLIT
            }
        }
        
        metadata_path = self.config.GOLD_DIR / "metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"Métadonnées sauvegardées: {metadata_path}")