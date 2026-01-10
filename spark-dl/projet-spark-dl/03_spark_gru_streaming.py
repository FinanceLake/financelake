# 03_spark_gru_streaming.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
import tensorflow as tf
import joblib
from collections import deque
import time

print("="*60)
print("🚀 SPARK STREAMING AVEC GRU")
print("="*60)

# Charger le modèle GRU
print("🤖 Chargement du modèle...")
try:
    model = tf.keras.models.load_model('models/gru_model.h5')
    scaler = joblib.load('models/scaler.pkl')
    print("✅ Modèle GRU chargé")
except:
    print("❌ Exécute d'abord: python 01_train_gru.py")
    exit()

# Buffers pour chaque symbole
buffers = {symbol: deque(maxlen=10) for symbol in ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']}

# Configuration Spark
spark = SparkSession.builder \
    .appName("StockGRUStreaming") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema des données
schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("hour", IntegerType()),
    StructField("minute", IntegerType()),
    StructField("price_change", DoubleType())
])

# Lire depuis Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock-prices") \
    .option("startingOffsets", "latest") \
    .load()

# Parser JSON
stocks_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*") \
.withColumn("event_time", to_timestamp(col("timestamp")))

print("✅ Connexion Kafka établie")

# Fonction de prédiction
def predict_with_gru(symbol, price, volume, hour, minute, price_change):
    """Prédit si le prix va monter avec GRU"""
    try:
        # Mettre à jour le buffer
        buffers[symbol].append({
            'price': price, 'volume': volume, 'price_change': price_change,
            'hour': hour, 'minute': minute
        })
        
        # Vérifier si on a assez de données
        if len(buffers[symbol]) < 10:
            return None, None
        
        # Préparer les features
        features = []
        for data in list(buffers[symbol]):
            # Créer des features simples
            returns = data['price_change'] / 100 if data['price_change'] != 0 else 0
            hour_norm = (data['hour'] - 9) / 7  # Normaliser 9-16h -> 0-1
            
            features.append([
                data['price'], data['volume'], data['price_change'],
                returns, hour_norm, data['minute']/60, 0  # placeholder
            ])
        
        # Normaliser et prédire
        features_array = np.array(features).reshape(1, 10, 7)
        features_scaled = scaler.transform(features_array.reshape(-1, 7)).reshape(1, 10, 7)
        
        prediction = model.predict(features_scaled, verbose=0)[0][0]
        
        # Interpréter
        trend = "📈 HAUSSE" if prediction > 0.5 else "📉 BAISSE"
        confidence = prediction if prediction > 0.5 else 1 - prediction
        
        return trend, float(confidence)
        
    except Exception as e:
        return None, None

# UDF Spark pour les prédictions
def predict_udf(symbol, price, volume, hour, minute, price_change):
    trend, confidence = predict_with_gru(symbol, price, volume, hour, minute, price_change)
    
    if trend:
        return {
            'prediction': trend,
            'confidence': confidence,
            'action': 'ACHAT' if 'HAUSSE' in trend and confidence > 0.7 else
                     'VENTE' if 'BAISSE' in trend and confidence > 0.7 else 'ATTENTE'
        }
    else:
        return {
            'prediction': '⏳ DONNÉES INSUFFISANTES',
            'confidence': 0.0,
            'action': 'ATTENTE'
        }

# Enregistrer UDF
predict_udf_spark = udf(predict_udf, MapType(StringType(), StringType()))

# Ajouter les prédictions
stocks_with_pred = stocks_df \
    .withColumn("gru_pred", predict_udf_spark(
        col("symbol"), col("price"), col("volume"),
        col("hour"), col("minute"), col("price_change")
    )) \
    .withColumn("prediction", col("gru_pred.prediction")) \
    .withColumn("confidence", col("gru_pred.confidence").cast(DoubleType())) \
    .withColumn("action", col("gru_pred.action"))

# Fonction de traitement
def process_batch(batch_df, batch_id):
    if batch_df.count() > 0:
        print(f"\n{'='*60}")
        print(f"📦 BATCH {batch_id} - {batch_df.count()} messages")
        print(f"⏰ {time.strftime('%H:%M:%S')}")
        print(f"{'='*60}")
        
        # Afficher les prédictions
        predictions = batch_df.filter(col("prediction") != "⏳ DONNÉES INSUFFISANTES")
        
        if predictions.count() > 0:
            print("\n🤖 PRÉDICTIONS GRU:")
            predictions.select(
                "symbol",
                round("price", 2).alias("prix"),
                "prediction",
                round("confidence", 2).alias("confiance"),
                "action",
                date_format("event_time", "HH:mm:ss").alias("heure")
            ).show(truncate=False)
            
            # Alertes
            print("\n🚨 ALERTES (Confiance > 0.7):")
            alerts = predictions.filter(col("confidence") > 0.7)
            if alerts.count() > 0:
                alerts.select("symbol", "action", "prediction", 
                            round("confidence", 2).alias("conf")).show(truncate=False)
        else:
            print("⏳ En attente de données...")
        
        # Stats
        print(f"\n📊 STATISTIQUES:")
        batch_df.groupBy("symbol").agg(
            round(avg("price"), 2).alias("prix_moyen"),
            round(stddev("price"), 2).alias("volatilite"),
            count("*").alias("n_messages")
        ).show(truncate=False)
        
        print(f"\n✅ Batch traité")
        print(f"{'='*60}\n")

# Démarrer le streaming
print("\n🎬 Démarrage du streaming...")
query = stocks_with_pred \
    .writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .start()

# Interface simple
print("\n🎮 Commandes:")
print("  q - Quitter")
print("-" * 40)

try:
    while True:
        cmd = input().strip().lower()
        if cmd == 'q':
            print("\n🛑 Arrêt...")
            query.stop()
            break
except:
    query.stop()

print("👋 Terminé")

