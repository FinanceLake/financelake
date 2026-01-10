# 01_train_gru.py
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import GRU, Dense, Dropout
from sklearn.preprocessing import MinMaxScaler
import joblib
import os

print("="*60)
print("🤖 ENTRAÎNEMENT DU MODÈLE GRU SIMPLIFIÉ")
print("="*60)

# Créer dossier models
os.makedirs('models', exist_ok=True)

class SimpleGRUModel:
    def __init__(self):
        self.model = None
        self.scaler = MinMaxScaler()
        self.sequence_length = 10
        
    def generate_data(self):
        """Génère des données d'entraînement simulées"""
        print("📊 Génération des données d'entraînement...")
        
        # Données pour 5 symboles
        symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']
        all_data = []
        
        for symbol in symbols:
            # 1000 points par symbole
            n_points = 1000
            time = np.arange(n_points)
            
            # Prix de base
            base_prices = {'AAPL': 150, 'GOOGL': 2800, 'MSFT': 330, 
                          'TSLA': 200, 'AMZN': 3500}
            base = base_prices[symbol]
            
            # Générer série temporelle réaliste
            trend = 0.0001 * time
            seasonal = 0.02 * np.sin(2 * np.pi * time / 50)
            noise = np.random.normal(0, 0.01, n_points)
            
            prices = base * (1 + trend + seasonal + noise)
            volumes = np.random.randint(1000, 50000, n_points)
            
            # Créer features simples
            df = pd.DataFrame({
                'symbol': symbol,
                'price': prices,
                'volume': volumes,
                'price_change': np.concatenate([[0], np.diff(prices)]),
                'returns': np.concatenate([[0], np.diff(prices) / prices[:-1]]),
                'hour': np.random.randint(9, 16, n_points),
                'minute': np.random.randint(0, 60, n_points)
            })
            
            # Moyennes mobiles
            df['ma_5'] = df['price'].rolling(5).mean().fillna(method='bfill')
            df['ma_10'] = df['price'].rolling(10).mean().fillna(method='bfill')
            df['volatility'] = df['returns'].rolling(5).std().fillna(0)
            
            all_data.append(df)
        
        return pd.concat(all_data, ignore_index=True)
    
    def create_sequences(self, df):
        """Crée des séquences pour GRU"""
        sequences = []
        targets = []
        
        for symbol in df['symbol'].unique():
            symbol_data = df[df['symbol'] == symbol]
            
            # Features à utiliser
            features = ['price', 'volume', 'price_change', 'returns', 
                       'ma_5', 'ma_10', 'volatility']
            feature_data = symbol_data[features].values
            
            for i in range(len(feature_data) - self.sequence_length - 5):
                # Séquence d'entrée
                seq = feature_data[i:i + self.sequence_length]
                
                # Target: prix dans 5 minutes (1=hausse, 0=baisse)
                current_price = feature_data[i + self.sequence_length - 1, 0]
                future_price = feature_data[i + self.sequence_length + 4, 0]
                target = 1 if future_price > current_price else 0
                
                sequences.append(seq)
                targets.append(target)
        
        return np.array(sequences), np.array(targets)
    
    def build_model(self):
        """Construit le modèle GRU"""
        print("🏗️ Construction du modèle...")
        
        model = Sequential([
            GRU(64, input_shape=(self.sequence_length, 7), 
                return_sequences=True, dropout=0.2),
            GRU(32, dropout=0.2),
            Dense(16, activation='relu'),
            Dropout(0.3),
            Dense(1, activation='sigmoid')
        ])
        
        model.compile(
            optimizer='adam',
            loss='binary_crossentropy',
            metrics=['accuracy', 'AUC']
        )
        
        return model
    
    def train(self):
        """Entraîne le modèle"""
        # Générer données
        df = self.generate_data()
        
        # Normaliser
        print("📐 Normalisation des données...")
        features = ['price', 'volume', 'price_change', 'returns', 
                   'ma_5', 'ma_10', 'volatility']
        df[features] = self.scaler.fit_transform(df[features])
        
        # Créer séquences
        print("📈 Création des séquences...")
        X, y = self.create_sequences(df)
        
        print(f"✅ Données préparées: {X.shape[0]} séquences")
        
        # Split train/test
        split = int(len(X) * 0.8)
        X_train, X_test = X[:split], X[split:]
        y_train, y_test = y[:split], y[split:]
        
        # Construire et entraîner
        self.model = self.build_model()
        
        print("🔥 Début de l'entraînement...")
        history = self.model.fit(
            X_train, y_train,
            validation_data=(X_test, y_test),
            epochs=20,
            batch_size=32,
            verbose=1
        )
        
        # Évaluation
        test_loss, test_acc, test_auc = self.model.evaluate(X_test, y_test, verbose=0)
        print(f"\n📊 Résultats sur le test set:")
        print(f"   Accuracy: {test_acc:.4f}")
        print(f"   AUC: {test_auc:.4f}")
        print(f"   Loss: {test_loss:.4f}")
        
        # Sauvegarder
        self.model.save('models/gru_model.h5')
        joblib.dump(self.scaler, 'models/scaler.pkl')
        
        print("\n💾 Modèle sauvegardé dans 'models/gru_model.h5'")
        
        return history

if __name__ == "__main__":
    # Entraîner le modèle
    trainer = SimpleGRUModel()
    trainer.train()
    
    print("\n" + "="*60)
    print("✅ ENTRAÎNEMENT TERMINÉ !")
    print("="*60)
