import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.spark_config import create_spark_session
import numpy as np
import pandas as pd
import joblib
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from config.spark_config import create_spark_session

# Configuration
GOLD_PATH = "hdfs://localhost:9000/financelake/gold"
MODEL_DIR = "analytics/models/"
LOOKBACK = 7

# Créer le dossier pour les modèles s'il n'existe pas
if not os.path.exists(MODEL_DIR):
    os.makedirs(MODEL_DIR)

spark = create_spark_session()

print(" Lecture de la table Gold...")
df_gold = spark.read.format("delta").load(GOLD_PATH)
pdf = df_gold.toPandas()

tickers = pdf['ticker'].unique()
print(f"Stock trouvés : {tickers}")

for t in tickers:
    print(f"\n---  Entraînement du modèle pour : {t} ---")
    
    # Filtrer les données pour ce ticker et trier par date
    data_ticker = pdf[pdf['ticker'] == t].sort_values('trading_date')
    values = data_ticker[["avg_price", "total_volume"]].values
    
    if len(values) <= LOOKBACK:
        print(f" Pas assez de données pour {t} ({len(values)} lignes).")
        continue

    # 3. Normalisation 
    scaler = MinMaxScaler()
    scaled_data = scaler.fit_transform(values)
    joblib.dump(scaler, f"{MODEL_DIR}scaler_{t}.gz")

    # 4. Création des séquences
    X, y = [], []
    for i in range(LOOKBACK, len(scaled_data)):
        X.append(scaled_data[i-LOOKBACK:i])
        y.append(scaled_data[i, 0])
    
    X, y = np.array(X), np.array(y)

    # 5. Définition du modèle
    model = Sequential([
        LSTM(64, return_sequences=True, input_shape=(LOOKBACK, 2)),
        Dropout(0.2),
        LSTM(32),
        Dense(1)
    ])
    model.compile(optimizer='adam', loss='mse')

    # 6. Entraînement 
    model.fit(X, y, epochs=15, batch_size=16, verbose=0)
    
    # 7. Sauvegarde
    model.save(f"{MODEL_DIR}model_{t}.h5")
    print(f" Modèle et Scaler sauvegardés pour {t}")

print("\n Tous les modèles ont été entraînés !")
