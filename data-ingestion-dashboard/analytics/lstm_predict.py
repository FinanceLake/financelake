import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.spark_config import create_spark_session
import numpy as np
import joblib
from tensorflow.keras.models import load_model
from config.spark_config import create_spark_session
from pyspark.sql.functions import col

GOLD_PATH = "hdfs://localhost:9000/financelake/gold"
MODEL_DIR = "analytics/models/"
LOOKBACK = 7

spark = create_spark_session()

def predict_ticker(ticker_name):
    model_path = f"{MODEL_DIR}model_{ticker_name}.h5"
    scaler_path = f"{MODEL_DIR}scaler_{ticker_name}.gz"
    
    if not os.path.exists(model_path):
        print(f" Aucun modèle trouvé pour {ticker_name}")
        return

    # 1. Récupérer les 7 derniers jours depuis Gold
    df_last = spark.read.format("delta").load(GOLD_PATH) \
        .filter(col("ticker") == ticker_name) \
        .orderBy(col("trading_date").desc()) \
        .limit(LOOKBACK)
    
    pdf = df_last.toPandas()
    
    if len(pdf) < LOOKBACK:
        print(f" Données insuffisantes pour prédire {ticker_name}")
        return

    # Remettre dans l'ordre chronologique
    pdf = pdf.iloc[::-1]
    
    # 2. Charger le modèle et le scaler
    model = load_model(model_path)
    scaler = joblib.load(scaler_path)
    
    # 3. Préparer l'entrée
    input_data = scaler.transform(pdf[["avg_price", "total_volume"]])
    X_input = input_data.reshape(1, LOOKBACK, 2)
    
    # 4. Prédire
    pred_scaled = model.predict(X_input, verbose=0)
    
    # Re-transformer en prix réel
    dummy = np.zeros((1, 2))
    dummy[0, 0] = pred_scaled
    price_final = scaler.inverse_transform(dummy)[0, 0]
    
    print(f" {ticker_name} : Prix prédit pour demain = {price_final:.2f} $")

# --- EXECUTION ---

# Récupérer la liste des tickers dans Gold pour tout traiter
all_tickers = spark.read.format("delta").load(GOLD_PATH) \
    .select("ticker").distinct().toPandas()["ticker"].tolist()

print(f" Lancement des prédictions pour : {all_tickers}\n")

for t in all_tickers:
    predict_ticker(t)