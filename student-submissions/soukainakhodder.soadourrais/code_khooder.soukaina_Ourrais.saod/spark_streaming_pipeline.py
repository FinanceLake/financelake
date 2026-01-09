"""
Spark Structured Streaming Pipeline - Tâche 1
Pipeline d'ingestion et traitement en temps réel des données boursières
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, avg, stddev, count, 
    max as spark_max, min as spark_min, sum as spark_sum, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from config import WINDOW_DURATION, SLIDE_DURATION, CHECKPOINT_DIR, DELTA_BRONZE_PATH, DELTA_CHECKPOINT_DIR
import os

class StockStreamingPipeline:
    """
    Pipeline de streaming Spark pour l'analyse en temps réel des actions boursières
    """
    
    def __init__(self, app_name="RealTimeStockInsight", enable_delta=False):
        """
        Initialise la session Spark avec les configurations optimales
        
        Args:
            app_name: Nom de l'application Spark
            enable_delta: Si True, active le support Delta Lake
        """
        builder = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.driver.host", "localhost")
        
        # Ajouter support Delta Lake si demandé
        if enable_delta:
            builder = builder \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        self.spark = builder.getOrCreate()
        
        # Réduire la verbosité des logs
        self.spark.sparkContext.setLogLevel("ERROR")
        
        # Liste pour suivre les queries actives
        self.active_queries = []
        
        self.enable_delta = enable_delta
        print("✅ Session Spark initialisée avec succès")
        if enable_delta:
            print("✅ Support Delta Lake activé")
    
    def define_schema(self):
        """
        Définit le schéma explicite pour les données JSON entrantes
        
        Schema: symbol, price, volume, timestamp, price_change
        """
        schema = StructType([
            StructField("symbol", StringType(), False),
            StructField("price", DoubleType(), False),
            StructField("volume", IntegerType(), False),
            StructField("timestamp", StringType(), False),
            StructField("price_change", DoubleType(), False)
        ])
        return schema
    
    def create_streaming_dataframe(self, input_path="./stream_data"):
        """
        Crée un DataFrame en streaming à partir de fichiers JSON
        
        Args:
            input_path: Chemin vers le dossier contenant les fichiers JSON
            
        Returns:
            DataFrame en streaming avec schéma parsé
        """
        schema = self.define_schema()
        
        # Lecture du flux de fichiers JSON
        streaming_df = self.spark \
            .readStream \
            .schema(schema) \
            .json(input_path)
        
        # Conversion du timestamp string en TimestampType
        streaming_df = streaming_df.withColumn(
            "timestamp", 
            col("timestamp").cast(TimestampType())
        )
        
        print(f"📊 DataFrame en streaming créé depuis: {input_path}")
        print("📋 Schéma défini:")
        streaming_df.printSchema()
        
        return streaming_df
    
    def apply_windowed_aggregations(self, streaming_df):
        """
        Applique des agrégations glissantes par fenêtre temporelle
        
        Calcule pour chaque symbole et fenêtre de temps:
        - Prix moyen
        - Volatilité (écart-type des prix)
        - Volume total
        - Nombre de transactions
        - Prix min/max
        
        Args:
            streaming_df: DataFrame en streaming
            
        Returns:
            DataFrame agrégé par fenêtre temporelle
        """
        windowed_df = streaming_df \
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
                avg("price_change").alias("avg_price_change")
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
                "avg_price_change"
            )
        
        print(f"🔄 Agrégations configurées:")
        print(f"   - Fenêtre: {WINDOW_DURATION}")
        print(f"   - Glissement: {SLIDE_DURATION}")
        print(f"   - Métriques: prix moyen, volatilité, volume, min/max")
        
        return windowed_df
    
    def write_to_console(self, df, query_name="console_output"):
        """
        Écrit le flux de sortie vers la console (pour debugging)
        """
        query = df.writeStream \
            .outputMode("complete") \
            .format("console") \
            .queryName(query_name) \
            .option("truncate", "false") \
            .trigger(processingTime='5 seconds') \
            .start()
        
        self.active_queries.append(query)
        return query
    
    def write_to_memory(self, df, table_name="stock_aggregates"):
        """
        Écrit le flux vers une table en mémoire pour interrogation SQL
        
        Args:
            df: DataFrame à écrire
            table_name: Nom de la table en mémoire
            
        Returns:
            StreamingQuery
        """
        query = df.writeStream \
            .outputMode("complete") \
            .format("memory") \
            .queryName(table_name) \
            .trigger(processingTime='5 seconds') \
            .start()
        
        print(f"💾 Flux écrit vers la table en mémoire: {table_name}")
        
        self.active_queries.append(query)
        return query
    
    def write_to_parquet(self, df, output_path="./output/stock_data"):
        """
        Écrit le flux vers des fichiers Parquet pour analyse batch ultérieure
        
        Args:
            df: DataFrame à écrire
            output_path: Chemin de sortie
            
        Returns:
            StreamingQuery
        """
        os.makedirs(output_path, exist_ok=True)
        
        query = df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/parquet") \
            .trigger(processingTime='10 seconds') \
            .start()
        
        print(f"💾 Flux écrit vers Parquet: {output_path}")
        
        self.active_queries.append(query)
        return query
    
    def write_to_delta(self, df, output_path=None, table_name="bronze_stock_data"):
        """
        Écrit le flux vers une table Delta Lake
        
        Args:
            df: DataFrame à écrire
            output_path: Chemin de sortie (défaut: DELTA_BRONZE_PATH)
            table_name: Nom de la table pour le checkpoint
            
        Returns:
            StreamingQuery
        """
        if not self.enable_delta:
            print("⚠️  Delta Lake non activé. Utilisez enable_delta=True lors de l'initialisation.")
            return None
        
        if output_path is None:
            output_path = DELTA_BRONZE_PATH
        
        os.makedirs(output_path, exist_ok=True)
        os.makedirs(DELTA_CHECKPOINT_DIR, exist_ok=True)
        
        # Ajouter timestamp d'ingestion
        df_with_timestamp = df.withColumn("ingestion_timestamp", current_timestamp())
        
        query = df_with_timestamp.writeStream \
            .outputMode("append") \
            .format("delta") \
            .option("path", output_path) \
            .option("checkpointLocation", f"{DELTA_CHECKPOINT_DIR}/{table_name}") \
            .trigger(processingTime='5 seconds') \
            .start()
        
        print(f"💾 Flux écrit vers Delta Lake: {output_path}")
        
        self.active_queries.append(query)
        return query
    
    def run_pipeline(self, input_path="./stream_data", duration_seconds=300):
        """
        Exécute le pipeline complet
        
        Args:
            input_path: Chemin des données sources
            duration_seconds: Durée d'exécution (None = indéfini)
        """
        print("\n" + "="*60)
        print("🚀 DÉMARRAGE DU PIPELINE SPARK STREAMING")
        print("="*60 + "\n")
        
        # Créer le DataFrame en streaming
        streaming_df = self.create_streaming_dataframe(input_path)
        
        # Appliquer les agrégations
        aggregated_df = self.apply_windowed_aggregations(streaming_df)
        
        # Écrire vers plusieurs destinations
        console_query = self.write_to_console(aggregated_df, "console_output")
        memory_query = self.write_to_memory(aggregated_df, "stock_aggregates")
        
        # Optionnel: écrire vers Parquet pour analyse batch
        # parquet_query = self.write_to_parquet(streaming_df, "./output/stock_data")
        
        print("\n✅ Pipeline démarré avec succès!")
        print("📊 Les données agrégées sont disponibles dans la table 'stock_aggregates'")
        print(f"⏱️  Durée prévue: {duration_seconds} secondes\n")
        
        # Attendre la fin du traitement
        try:
            if duration_seconds:
                console_query.awaitTermination(timeout=duration_seconds)
            else:
                console_query.awaitTermination()
        except KeyboardInterrupt:
            print("\n⚠️  Interruption du pipeline...")
        finally:
            self.stop()
    
    def stop(self):
        """
        Arrête proprement la session Spark et toutes les queries actives
        """
        import time
        
        # Arrêter toutes les queries en streaming
        print("🛑 Arrêt des queries en streaming...")
        for query in self.active_queries:
            try:
                if query and query.isActive:
                    query.stop()
                    print(f"  ✓ Query '{query.name}' arrêtée")
            except Exception as e:
                print(f"  ⚠️  Erreur lors de l'arrêt de la query: {e}")
        
        # Attendre un peu pour que les ressources se libèrent
        time.sleep(2)
        
        # Arrêter la session Spark
        if self.spark:
            try:
                self.spark.stop()
                print("🛑 Session Spark arrêtée proprement")
            except Exception as e:
                print(f"⚠️  Erreur lors de l'arrêt de Spark (peut être ignorée): {e}")
    
    def get_spark_session(self):
        """
        Retourne la session Spark pour utilisation externe
        """
        return self.spark

if __name__ == "__main__":
    pipeline = StockStreamingPipeline()
    pipeline.run_pipeline(duration_seconds=120)  # Exécuter pendant 2 minutes

