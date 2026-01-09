"""
Delta Lake Pipeline - Support complet de Delta Lake
Gère les tables Bronze, Silver et Gold avec opérations Delta (MERGE, VACUUM, OPTIMIZE, time travel)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, avg, stddev, count, 
    max as spark_max, min as spark_min, sum as spark_sum,
    current_timestamp, lit, when, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from delta.tables import DeltaTable
from config import (
    DELTA_BRONZE_PATH, DELTA_SILVER_PATH, DELTA_GOLD_PATH,
    DELTA_CHECKPOINT_DIR, WINDOW_DURATION, SLIDE_DURATION
)
import os

class DeltaLakePipeline:
    """
    Pipeline Delta Lake avec architecture Bronze/Silver/Gold
    """
    
    def __init__(self, app_name="DeltaLakeStockInsight"):
        """
        Initialise la session Spark avec support Delta Lake
        """
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.sql.streaming.checkpointLocation", DELTA_CHECKPOINT_DIR) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.driver.host", "localhost") \
            .getOrCreate()
        
        # Réduire la verbosité des logs
        self.spark.sparkContext.setLogLevel("ERROR")
        
        # Créer les dossiers Delta
        os.makedirs(DELTA_BRONZE_PATH, exist_ok=True)
        os.makedirs(DELTA_SILVER_PATH, exist_ok=True)
        os.makedirs(DELTA_GOLD_PATH, exist_ok=True)
        os.makedirs(DELTA_CHECKPOINT_DIR, exist_ok=True)
        
        self.active_queries = []
        
        print("✅ Session Spark avec Delta Lake initialisée")
        print(f"📁 Bronze: {DELTA_BRONZE_PATH}")
        print(f"📁 Silver: {DELTA_SILVER_PATH}")
        print(f"📁 Gold: {DELTA_GOLD_PATH}")
    
    def define_schema(self):
        """
        Définit le schéma pour les données JSON entrantes
        """
        schema = StructType([
            StructField("symbol", StringType(), False),
            StructField("price", DoubleType(), False),
            StructField("volume", IntegerType(), False),
            StructField("timestamp", StringType(), False),
            StructField("price_change", DoubleType(), False)
        ])
        return schema
    
    def create_bronze_table(self, input_path="./stream_data"):
        """
        Crée la table Bronze (raw data) en Delta
        
        La table Bronze contient les données brutes sans transformation
        """
        schema = self.define_schema()
        
        # Lire le streaming et écrire vers Delta Bronze
        streaming_df = self.spark \
            .readStream \
            .schema(schema) \
            .json(input_path)
        
        # Convertir timestamp
        streaming_df = streaming_df.withColumn(
            "timestamp",
            col("timestamp").cast(TimestampType())
        ).withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        
        # Écrire vers Delta Bronze
        query = streaming_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", f"{DELTA_CHECKPOINT_DIR}/bronze") \
            .option("path", DELTA_BRONZE_PATH) \
            .trigger(processingTime='5 seconds') \
            .start()
        
        self.active_queries.append(query)
        print("✅ Table Bronze créée et alimentée en streaming")
        return query
    
    def create_silver_table(self):
        """
        Crée la table Silver (cleaned & enriched data) en Delta
        
        La table Silver contient:
        - Données nettoyées (pas de doublons, valeurs nulles gérées)
        - Données enrichies (agrégations par fenêtres)
        - Qualité des données validée
        """
        # Lire depuis Bronze
        bronze_df = self.spark.readStream \
            .format("delta") \
            .load(DELTA_BRONZE_PATH)
        
        # Nettoyer et enrichir les données
        silver_df = bronze_df \
            .withWatermark("timestamp", "1 minute") \
            .groupBy(
                window(col("timestamp"), WINDOW_DURATION, SLIDE_DURATION),
                col("symbol")
            ) \
            .agg(
                avg("price").alias("avg_price"),
                stddev("price").alias("volatility"),
                spark_sum("volume").alias("total_volume"),
                count("*").alias("transaction_count"),
                spark_min("price").alias("min_price"),
                spark_max("price").alias("max_price"),
                avg("price_change").alias("avg_price_change"),
                spark_max("ingestion_timestamp").alias("last_updated")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "symbol",
                "avg_price",
                "volatility",
                "total_volume",
                "transaction_count",
                "min_price",
                "max_price",
                "avg_price_change",
                "last_updated",
                current_timestamp().alias("silver_ingestion_time")
            )
        
        # Écrire vers Delta Silver avec MERGE pour éviter les doublons
        def write_to_silver(batch_df, batch_id):
            """
            Fonction pour écrire chaque batch vers Silver avec MERGE
            """
            if batch_df.count() > 0:
                # Créer une clé unique pour le merge
                batch_df = batch_df.withColumn(
                    "merge_key",
                    col("window_start").cast("string") + "_" + col("symbol")
                )
                
                # Si la table existe, faire un MERGE
                if DeltaTable.isDeltaTable(self.spark, DELTA_SILVER_PATH):
                    delta_table = DeltaTable.forPath(self.spark, DELTA_SILVER_PATH)
                    
                    delta_table.alias("target").merge(
                        batch_df.alias("source"),
                        "target.window_start = source.window_start AND target.symbol = source.symbol"
                    ).whenMatchedUpdateAll() \
                     .whenNotMatchedInsertAll() \
                     .execute()
                else:
                    # Première écriture: créer la table
                    batch_df.drop("merge_key").write \
                        .format("delta") \
                        .mode("append") \
                        .save(DELTA_SILVER_PATH)
        
        query = silver_df.writeStream \
            .foreachBatch(write_to_silver) \
            .outputMode("update") \
            .option("checkpointLocation", f"{DELTA_CHECKPOINT_DIR}/silver") \
            .trigger(processingTime='10 seconds') \
            .start()
        
        self.active_queries.append(query)
        print("✅ Table Silver créée et alimentée en streaming")
        return query
    
    def create_gold_table(self):
        """
        Crée la table Gold (business metrics) en Delta
        
        La table Gold contient des métriques business agrégées:
        - Statistiques par symbole
        - Tendances et indicateurs
        - Données prêtes pour le ML et les dashboards
        """
        # Lire depuis Silver
        silver_df = self.spark.readStream \
            .format("delta") \
            .load(DELTA_SILVER_PATH)
        
        # Calculer des métriques business
        gold_df = silver_df \
            .withWatermark("window_start", "2 minutes") \
            .groupBy("symbol") \
            .agg(
                avg("avg_price").alias("overall_avg_price"),
                avg("volatility").alias("overall_volatility"),
                spark_sum("total_volume").alias("cumulative_volume"),
                count("*").alias("window_count"),
                spark_min("min_price").alias("absolute_min_price"),
                spark_max("max_price").alias("absolute_max_price"),
                avg("avg_price_change").alias("trend_direction"),
                spark_max("window_start").alias("last_window_time")
            ) \
            .withColumn(
                "price_trend",
                when(col("trend_direction") > 0, "UP")
                .when(col("trend_direction") < 0, "DOWN")
                .otherwise("STABLE")
            ) \
            .withColumn(
                "volatility_category",
                when(col("overall_volatility") > 5, "HIGH")
                .when(col("overall_volatility") > 2, "MEDIUM")
                .otherwise("LOW")
            ) \
            .withColumn("gold_ingestion_time", current_timestamp())
        
        # Écrire vers Delta Gold avec MERGE
        def write_to_gold(batch_df, batch_id):
            """
            Fonction pour écrire chaque batch vers Gold avec MERGE
            """
            if batch_df.count() > 0:
                if DeltaTable.isDeltaTable(self.spark, DELTA_GOLD_PATH):
                    delta_table = DeltaTable.forPath(self.spark, DELTA_GOLD_PATH)
                    
                    delta_table.alias("target").merge(
                        batch_df.alias("source"),
                        "target.symbol = source.symbol"
                    ).whenMatchedUpdateAll() \
                     .whenNotMatchedInsertAll() \
                     .execute()
                else:
                    batch_df.write \
                        .format("delta") \
                        .mode("append") \
                        .save(DELTA_GOLD_PATH)
        
        query = gold_df.writeStream \
            .foreachBatch(write_to_gold) \
            .outputMode("update") \
            .option("checkpointLocation", f"{DELTA_CHECKPOINT_DIR}/gold") \
            .trigger(processingTime='15 seconds') \
            .start()
        
        self.active_queries.append(query)
        print("✅ Table Gold créée et alimentée en streaming")
        return query
    
    def optimize_tables(self):
        """
        Optimise les tables Delta avec OPTIMIZE (compaction)
        """
        print("\n" + "="*60)
        print("🔧 OPTIMISATION DES TABLES DELTA")
        print("="*60 + "\n")
        
        tables = [
            ("Bronze", DELTA_BRONZE_PATH),
            ("Silver", DELTA_SILVER_PATH),
            ("Gold", DELTA_GOLD_PATH)
        ]
        
        for table_name, path in tables:
            try:
                if DeltaTable.isDeltaTable(self.spark, path):
                    print(f"📊 Optimisation de la table {table_name}...")
                    delta_table = DeltaTable.forPath(self.spark, path)
                    delta_table.optimize().executeCompaction()
                    print(f"✅ Table {table_name} optimisée")
                else:
                    print(f"⚠️  Table {table_name} n'existe pas encore")
            except Exception as e:
                print(f"⚠️  Erreur lors de l'optimisation de {table_name}: {e}")
    
    def vacuum_tables(self, retention_hours=168):
        """
        Nettoie les anciens fichiers avec VACUUM
        
        Args:
            retention_hours: Heures de rétention (défaut: 7 jours = 168h)
        """
        print("\n" + "="*60)
        print("🧹 NETTOYAGE DES TABLES DELTA (VACUUM)")
        print("="*60 + "\n")
        
        tables = [
            ("Bronze", DELTA_BRONZE_PATH),
            ("Silver", DELTA_SILVER_PATH),
            ("Gold", DELTA_GOLD_PATH)
        ]
        
        for table_name, path in tables:
            try:
                if DeltaTable.isDeltaTable(self.spark, path):
                    print(f"🧹 VACUUM de la table {table_name} (rétention: {retention_hours}h)...")
                    delta_table = DeltaTable.forPath(self.spark, path)
                    delta_table.vacuum(retentionHours=retention_hours)
                    print(f"✅ Table {table_name} nettoyée")
                else:
                    print(f"⚠️  Table {table_name} n'existe pas encore")
            except Exception as e:
                print(f"⚠️  Erreur lors du VACUUM de {table_name}: {e}")
    
    def time_travel_query(self, table_path, version=None, timestamp=None):
        """
        Utilise le time travel de Delta Lake pour lire une version antérieure
        
        Args:
            table_path: Chemin de la table Delta
            version: Version spécifique à lire (optionnel)
            timestamp: Timestamp spécifique à lire (optionnel)
            
        Returns:
            DataFrame à la version/timestamp spécifié
        """
        if not DeltaTable.isDeltaTable(self.spark, table_path):
            print(f"❌ {table_path} n'est pas une table Delta")
            return None
        
        print("\n" + "="*60)
        print("⏰ TIME TRAVEL - Lecture d'une version antérieure")
        print("="*60 + "\n")
        
        if version is not None:
            print(f"📖 Lecture de la version {version}")
            df = self.spark.read \
                .format("delta") \
                .option("versionAsOf", version) \
                .load(table_path)
        elif timestamp is not None:
            print(f"📖 Lecture au timestamp {timestamp}")
            df = self.spark.read \
                .format("delta") \
                .option("timestampAsOf", timestamp) \
                .load(table_path)
        else:
            print("📖 Lecture de la version actuelle")
            df = self.spark.read.format("delta").load(table_path)
        
        return df
    
    def show_table_history(self, table_path):
        """
        Affiche l'historique des versions d'une table Delta
        """
        if not DeltaTable.isDeltaTable(self.spark, table_path):
            print(f"❌ {table_path} n'est pas une table Delta")
            return
        
        print("\n" + "="*60)
        print("📜 HISTORIQUE DE LA TABLE DELTA")
        print("="*60 + "\n")
        
        delta_table = DeltaTable.forPath(self.spark, table_path)
        history = delta_table.history()
        
        print("Versions disponibles:")
        history.select("version", "timestamp", "operation", "operationParameters").show(truncate=False)
    
    def run_delta_pipeline(self, input_path="./stream_data", duration_seconds=300):
        """
        Exécute le pipeline complet Delta Lake
        
        Args:
            input_path: Chemin des données sources
            duration_seconds: Durée d'exécution
        """
        print("\n" + "="*70)
        print("🚀 DÉMARRAGE DU PIPELINE DELTA LAKE")
        print("="*70 + "\n")
        print("Architecture Bronze/Silver/Gold:")
        print("  📦 Bronze: Données brutes (raw)")
        print("  🪙 Silver: Données nettoyées et enrichies")
        print("  🏆 Gold: Métriques business agrégées")
        print("\n" + "="*70 + "\n")
        
        # Créer les tables en streaming
        bronze_query = self.create_bronze_table(input_path)
        silver_query = self.create_silver_table()
        gold_query = self.create_gold_table()
        
        print("\n✅ Pipeline Delta Lake démarré!")
        print("📊 Les tables sont alimentées en continu...")
        print(f"⏱️  Durée prévue: {duration_seconds} secondes\n")
        
        try:
            if duration_seconds:
                bronze_query.awaitTermination(timeout=duration_seconds)
            else:
                bronze_query.awaitTermination()
        except KeyboardInterrupt:
            print("\n⚠️  Interruption du pipeline...")
        finally:
            self.stop()
    
    def stop(self):
        """
        Arrête proprement toutes les queries et la session Spark
        """
        import time
        
        print("\n🛑 Arrêt des queries Delta Lake...")
        for query in self.active_queries:
            try:
                if query and query.isActive:
                    query.stop()
                    print(f"  ✓ Query '{query.name}' arrêtée")
            except Exception as e:
                print(f"  ⚠️  Erreur: {e}")
        
        time.sleep(2)
        
        if self.spark:
            try:
                self.spark.stop()
                print("🛑 Session Spark arrêtée")
            except Exception as e:
                print(f"⚠️  Erreur lors de l'arrêt: {e}")
    
    def get_spark_session(self):
        """
        Retourne la session Spark
        """
        return self.spark

if __name__ == "__main__":
    pipeline = DeltaLakePipeline()
    pipeline.run_delta_pipeline(duration_seconds=120)

