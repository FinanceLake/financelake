"""
FinanceLake - Pipeline complet avec HDFS + Delta Lake
Architecture : Bronze → Silver → Gold
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import kagglehub

# ============================================
# 1. CONFIGURATION SPARK + DELTA + HDFS
# ============================================
spark = (
    SparkSession.builder
    .appName("FinanceLake")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "io.delta:delta-storage:3.2.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("✓ Spark session créée avec succès")
print(f"✓ Spark Master : {spark.sparkContext.master}")
print(f"✓ HDFS : {spark.conf.get('spark.hadoop.fs.defaultFS')}")
print("=" * 80)

# ============================================
# 2. CHEMINS HDFS POUR DELTA LAKE
# ============================================
BRONZE_BASE = "hdfs://localhost:9000/delta/bronze"
SILVER_BASE = "hdfs://localhost:9000/delta/silver"
GOLD_BASE = "hdfs://localhost:9000/delta/gold"

# Bronze paths
bronze_stock_trades = f"{BRONZE_BASE}/stock_trades"
bronze_crypto_prices = f"{BRONZE_BASE}/crypto_prices"
bronze_bank_transactions = f"{BRONZE_BASE}/bank_transactions"
bronze_credit_cards = f"{BRONZE_BASE}/credit_cards"
bronze_clients = f"{BRONZE_BASE}/clients"

# Silver paths
silver_trades_clean = f"{SILVER_BASE}/trades_clean"
silver_crypto_clean = f"{SILVER_BASE}/crypto_clean"
silver_transactions_clean = f"{SILVER_BASE}/transactions_clean"
silver_clients_enriched = f"{SILVER_BASE}/clients_enriched"

# Gold paths
gold_portfolio_pnl = f"{GOLD_BASE}/portfolio_pnl"
gold_fraud_scores = f"{GOLD_BASE}/fraud_scores"
gold_trading_volume = f"{GOLD_BASE}/trading_volume"

# ============================================
# 3. SCHÉMAS DE DONNÉES
# ============================================
stock_trades_schema = StructType([
    StructField("trade_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("side", StringType()),
    StructField("client_id", StringType()),
    StructField("exchange", StringType())
])

crypto_prices_schema = StructType([
    StructField("crypto_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("price_usd", DoubleType()),
    StructField("change_24h", DoubleType()),
    StructField("volume", LongType())
])

bank_transactions_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("client_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("type", StringType()),
    StructField("merchant", StringType()),
    StructField("card_last4", StringType()),
    StructField("is_fraud", IntegerType())
])

# ============================================
# 4. INGESTION BATCH : CREDIT CARDS (KAGGLE)
# ============================================
def ingest_batch_credit_cards():
    """
    Bronze : Télécharge et ingère le dataset Kaggle Credit Card Fraud
    """
    print("\n📦 [BATCH] Ingestion Credit Cards depuis Kaggle...")
    
    try:
        # Télécharger le dataset
        path = kagglehub.dataset_download("mlg-ulb/creditcardfraud")
        print(f"✓ Dataset téléchargé : {path}")
        
        # Lire le CSV
        df_credit_raw = spark.read.csv("file:///home/hadoop/.cache/kagglehub/datasets/mlg-ulb/creditcardfraud/versions/3/creditcard.csv", header=True, inferSchema=True)        
        print(f"✓ Lignes lues : {df_credit_raw.count()}")
        
        # Ajouter métadonnées d'ingestion
        df_credit_bronze = df_credit_raw \
            .withColumn("ingested_at", current_timestamp()) \
            .withColumn("data_source", lit("kaggle_creditcard"))
        
        # Écrire dans Bronze HDFS
        df_credit_bronze.write.format("delta") \
            .mode("overwrite") \
            .save(bronze_credit_cards)
        
        print(f"✓ Sauvegardé dans : {bronze_credit_cards}")
        
        return df_credit_bronze
        
    except Exception as e:
        print(f"❌ Erreur ingestion credit cards : {e}")
        return None

# ============================================
# 5. INGESTION STREAMING : KAFKA → BRONZE
# ============================================
def start_streaming_stock_trades():
    """
    Streaming : Kafka stock-trades → Bronze HDFS Delta
    """
    print("\n🔵 [STREAMING] Démarrage ingestion stock trades...")
    
    df_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "stock-trades")
        .option("startingOffsets", "latest")
        .load()
    )
    
    # Parser le JSON Kafka
    df_parsed = df_stream \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), stock_trades_schema).alias("data")) \
        .select("data.*") \
        .withColumn("ingested_at", current_timestamp())
    
    # Écrire dans Bronze HDFS
    query = (
        df_parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{BRONZE_BASE}/checkpoints/stock_trades")
        .trigger(processingTime="10 seconds")
        .start(bronze_stock_trades)
    )
    
    print(f"✓ Stream actif vers : {bronze_stock_trades}")
    return query

def start_streaming_crypto_prices():
    """
    Streaming : Kafka crypto-prices → Bronze
    """
    print("\n🟡 [STREAMING] Démarrage ingestion crypto prices...")
    
    df_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "crypto-prices")
        .option("startingOffsets", "latest")
        .load()
    )
    
    df_parsed = df_stream \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), crypto_prices_schema).alias("data")) \
        .select("data.*") \
        .withColumn("ingested_at", current_timestamp())
    
    query = (
        df_parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{BRONZE_BASE}/checkpoints/crypto_prices")
        .trigger(processingTime="30 seconds")
        .start(bronze_crypto_prices)
    )
    
    print(f"✓ Stream actif vers : {bronze_crypto_prices}")
    return query

def start_streaming_bank_transactions():
    """
    Streaming : Kafka bank-transactions → Bronze
    """
    print("\n🟢 [STREAMING] Démarrage ingestion bank transactions...")
    
    df_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "bank-transactions")
        .option("startingOffsets", "latest")
        .load()
    )
    
    df_parsed = df_stream \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), bank_transactions_schema).alias("data")) \
        .select("data.*") \
        .withColumn("ingested_at", current_timestamp())
    
    query = (
        df_parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{BRONZE_BASE}/checkpoints/bank_transactions")
        .trigger(processingTime="5 seconds")
        .start(bronze_bank_transactions)
    )
    
    print(f"✓ Stream actif vers : {bronze_bank_transactions}")
    return query

# ============================================
# 6. TRANSFORMATION : BRONZE → SILVER
# ============================================
def bronze_to_silver_trades():
    """
    Silver : Nettoyage et enrichissement des trades
    """
    print("\n🔄 [SILVER] Transformation stock trades...")
    
    # Lire depuis Bronze
    df_bronze = spark.read.format("delta").load(bronze_stock_trades)
    
    # Transformations
    df_silver = df_bronze \
        .withColumn("notional", col("price") * col("quantity")) \
        .withColumn("trade_date", to_date("timestamp")) \
        .withColumn("trade_hour", hour("timestamp")) \
        .dropDuplicates(["trade_id"]) \
        .filter(col("price") > 0) \
        .filter(col("quantity") > 0)
    
    # Écrire dans Silver (MERGE pour idempotence)
    if DeltaTable.isDeltaTable(spark, silver_trades_clean):
        delta_table = DeltaTable.forPath(spark, silver_trades_clean)
        
        (
            delta_table.alias("t")
            .merge(df_silver.alias("s"), "t.trade_id = s.trade_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print("✓ MERGE effectué dans Silver trades")
    else:
        df_silver.write.format("delta") \
            .partitionBy("trade_date") \
            .mode("overwrite") \
            .save(silver_trades_clean)
        print(f"✓ Silver trades créé : {silver_trades_clean}")
    
    return df_silver

def bronze_to_silver_transactions():
    """
    Silver : Nettoyage transactions + feature engineering pour ML
    """
    print("\n🔄 [SILVER] Transformation bank transactions...")
    
    df_bronze = spark.read.format("delta").load(bronze_bank_transactions)
    
    # Features pour détection de fraude
    df_silver = df_bronze \
        .withColumn("transaction_date", to_date("timestamp")) \
        .withColumn("transaction_hour", hour("timestamp")) \
        .withColumn("is_weekend", 
                   when(dayofweek("timestamp").isin([1, 7]), 1).otherwise(0)) \
        .withColumn("is_night", 
                   when((hour("timestamp") >= 22) | (hour("timestamp") <= 6), 1).otherwise(0)) \
        .withColumn("amount_log", log1p("amount")) \
        .dropDuplicates(["transaction_id"])
    
    # Calculer stats par client (window function)
    from pyspark.sql.window import Window
    
    window_spec = Window.partitionBy("client_id").orderBy("timestamp") \
        .rowsBetween(-10, 0)  # 10 dernières transactions
    
    df_silver = df_silver \
        .withColumn("avg_amount_last10", avg("amount").over(window_spec)) \
        .withColumn("std_amount_last10", stddev("amount").over(window_spec)) \
        .withColumn("deviation_score", 
                   abs(col("amount") - col("avg_amount_last10")) / 
                   (col("std_amount_last10") + 1))
    
    # Sauvegarder dans Silver
    df_silver.write.format("delta") \
        .partitionBy("transaction_date") \
        .mode("overwrite") \
        .save(silver_transactions_clean)
    
    print(f"✓ Silver transactions créé : {silver_transactions_clean}")
    return df_silver

# ============================================
# 7. AGRÉGATION : SILVER → GOLD
# ============================================
def silver_to_gold_trading_volume():
    """
    Gold : Volume de trading par symbole et heure
    """
    print("\n📊 [GOLD] Calcul trading volume...")
    
    df_silver = spark.read.format("delta").load(silver_trades_clean)
    
    df_gold = df_silver.groupBy("symbol", "trade_date", "trade_hour") \
        .agg(
            count("*").alias("num_trades"),
            sum("notional").alias("total_volume_usd"),
            avg("price").alias("avg_price"),
            min("price").alias("min_price"),
            max("price").alias("max_price")
        ) \
        .withColumn("volume_category",
                   when(col("total_volume_usd") > 1000000, "HIGH")
                   .when(col("total_volume_usd") > 100000, "MEDIUM")
                   .otherwise("LOW"))
    
    df_gold.write.format("delta") \
        .partitionBy("trade_date") \
        .mode("overwrite") \
        .save(gold_trading_volume)
    
    print(f"✓ Gold trading volume créé : {gold_trading_volume}")
    df_gold.show(10)
    return df_gold

def silver_to_gold_fraud_detection():
    """
    Gold : Scoring de risque de fraude (règles simples)
    """
    print("\n🚨 [GOLD] Calcul fraud scores...")
    
    df_silver = spark.read.format("delta").load(silver_transactions_clean)
    
    # Score de risque composite
    df_gold = df_silver \
        .withColumn("risk_score",
            (col("deviation_score") * 0.4) +
            (when(col("is_night") == 1, 30).otherwise(0)) +
            (when(col("is_weekend") == 1, 20).otherwise(0)) +
            (when(col("amount") > 5000, 50).otherwise(0))
        ) \
        .withColumn("risk_level",
            when(col("risk_score") > 80, "HIGH")
            .when(col("risk_score") > 50, "MEDIUM")
            .otherwise("LOW")
        ) \
        .select(
            "transaction_id", "client_id", "timestamp", 
            "amount", "type", "is_fraud",
            "deviation_score", "risk_score", "risk_level"
        )
    
    df_gold.write.format("delta") \
        .partitionBy("risk_level") \
        .mode("overwrite") \
        .save(gold_fraud_scores)
    
    print(f"✓ Gold fraud scores créé : {gold_fraud_scores}")
    
    # Statistiques de performance
    df_gold.groupBy("risk_level", "is_fraud").count().show()
    
    return df_gold

# ============================================
# 8. MONITORING & TIME TRAVEL
# ============================================
def show_delta_history(table_path):
    """
    Affiche l'historique Delta d'une table
    """
    print(f"\n📜 Historique Delta : {table_path}")
    DeltaTable.forPath(spark, table_path).history().select(
        "version", "timestamp", "operation", 
        "operationMetrics.numOutputRows"
    ).show(10, truncate=False)

def time_travel_example():
    """
    Démontre le Time Travel Delta
    """
    print("\n⏰ Exemple Time Travel...")
    
    # Lire version actuelle
    df_current = spark.read.format("delta").load(silver_trades_clean)
    count_current = df_current.count()
    
    # Lire version précédente
    df_previous = spark.read.format("delta") \
        .option("versionAsOf", 0) \
        .load(silver_trades_clean)
    count_previous = df_previous.count()
    
    print(f"Version 0 : {count_previous} lignes")
    print(f"Version actuelle : {count_current} lignes")
    print(f"Différence : {count_current - count_previous} nouvelles lignes")

# ============================================
# 9. MAIN : ORCHESTRATION
# ============================================
def main():
    print("\n" + "=" * 80)
    print("🚀 DÉMARRAGE FINANCELAKE - HDFS + DELTA LAKE")
    print("=" * 80)
    
    # Étape 1 : Ingestion Batch
    print("\n### PHASE 1 : INGESTION BATCH ###")
    ingest_batch_credit_cards()
    
    # Étape 2 : Démarrage Streaming
    print("\n### PHASE 2 : DÉMARRAGE STREAMING ###")
    query1 = start_streaming_stock_trades()
    query2 = start_streaming_crypto_prices()
    query3 = start_streaming_bank_transactions()
    
    # Attendre accumulation de données (30 secondes)
    print("\n⏳ Attente accumulation de données (30s)...")
    import time
    time.sleep(30)
    
    # Étape 3 : Transformations Bronze → Silver
    print("\n### PHASE 3 : TRANSFORMATIONS SILVER ###")
    bronze_to_silver_trades()
    bronze_to_silver_transactions()
    
    # Étape 4 : Agrégations Silver → Gold
    print("\n### PHASE 4 : AGRÉGATIONS GOLD ###")
    silver_to_gold_trading_volume()
    silver_to_gold_fraud_detection()
    
    # Étape 5 : Monitoring
    print("\n### PHASE 5 : MONITORING ###")
    show_delta_history(silver_trades_clean)
    time_travel_example()
    
    # Garder les streams actifs
    print("\n✅ Pipeline complet opérationnel !")
    print("Les streams Kafka continuent de s'exécuter...")
    print("Appuyez sur Ctrl+C pour arrêter\n")
    
    try:
        query1.awaitTermination()
    except KeyboardInterrupt:
        print("\n🛑 Arrêt des streams...")
        query1.stop()
        query2.stop()
        query3.stop()

if __name__ == "__main__":
    main()
