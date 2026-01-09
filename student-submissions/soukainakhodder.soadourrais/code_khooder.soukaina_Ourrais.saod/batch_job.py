"""
Batch Job - Historisation des données
Job batch pour traiter et historiser les données depuis les tables Delta
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, date_format, current_timestamp,
    avg, sum as spark_sum, count, max as spark_max, min as spark_min
)
from delta.tables import DeltaTable
from config import DELTA_BRONZE_PATH, DELTA_SILVER_PATH, DELTA_GOLD_PATH
import os

class BatchHistoricizationJob:
    """
    Job batch pour historiser les données depuis les tables Delta
    """
    
    def __init__(self, app_name="BatchHistoricization"):
        """
        Initialise la session Spark avec support Delta Lake
        """
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.ui.enabled", "false") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        
        # Dossier pour les données historisées
        self.historic_path = "./delta/historic"
        os.makedirs(self.historic_path, exist_ok=True)
        
        print("✅ Job batch d'historisation initialisé")
    
    def historize_bronze_data(self, date_partition=True):
        """
        Historise les données Bronze (raw data) avec partitionnement par date
        
        Args:
            date_partition: Si True, partitionne par date
        """
        print("\n" + "="*60)
        print("📦 HISTORISATION DES DONNÉES BRONZE")
        print("="*60 + "\n")
        
        if not DeltaTable.isDeltaTable(self.spark, DELTA_BRONZE_PATH):
            print(f"⚠️  Table Bronze n'existe pas encore: {DELTA_BRONZE_PATH}")
            return
        
        # Lire les données Bronze
        bronze_df = self.spark.read.format("delta").load(DELTA_BRONZE_PATH)
        
        print(f"📊 Nombre d'enregistrements à historiser: {bronze_df.count()}")
        
        if date_partition:
            # Ajouter des colonnes de partition par date
            bronze_df = bronze_df.withColumn("year", year(col("timestamp"))) \
                                 .withColumn("month", month(col("timestamp"))) \
                                 .withColumn("day", dayofmonth(col("timestamp"))) \
                                 .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))
        
        # Chemin de sortie
        historic_bronze_path = f"{self.historic_path}/bronze"
        
        # Écrire avec partitionnement par date si activé
        write_mode = bronze_df.write.format("delta").mode("overwrite")
        
        if date_partition:
            write_mode = write_mode.partitionBy("year", "month", "day")
        
        write_mode.save(historic_bronze_path)
        
        print(f"✅ Données Bronze historisées: {historic_bronze_path}")
        
        # Afficher un résumé
        historic_df = self.spark.read.format("delta").load(historic_bronze_path)
        print(f"📊 Enregistrements historisés: {historic_df.count()}")
        if date_partition:
            print("📅 Partitionnement par date: year/month/day")
    
    def historize_silver_data(self, date_partition=True):
        """
        Historise les données Silver (cleaned data) avec agrégations quotidiennes
        
        Args:
            date_partition: Si True, partitionne par date
        """
        print("\n" + "="*60)
        print("🪙 HISTORISATION DES DONNÉES SILVER")
        print("="*60 + "\n")
        
        if not DeltaTable.isDeltaTable(self.spark, DELTA_SILVER_PATH):
            print(f"⚠️  Table Silver n'existe pas encore: {DELTA_SILVER_PATH}")
            return
        
        # Lire les données Silver
        silver_df = self.spark.read.format("delta").load(DELTA_SILVER_PATH)
        
        print(f"📊 Nombre d'enregistrements à historiser: {silver_df.count()}")
        
        # Agréger par jour et symbole pour créer un résumé quotidien
        daily_summary = silver_df \
            .withColumn("date", date_format(col("window_start"), "yyyy-MM-dd")) \
            .groupBy("date", "symbol") \
            .agg(
                avg("avg_price").alias("daily_avg_price"),
                avg("volatility").alias("daily_avg_volatility"),
                spark_sum("total_volume").alias("daily_total_volume"),
                spark_sum("transaction_count").alias("daily_transaction_count"),
                spark_min("min_price").alias("daily_min_price"),
                spark_max("max_price").alias("daily_max_price"),
                avg("avg_price_change").alias("daily_avg_price_change"),
                spark_max("window_start").alias("last_window_time")
            ) \
            .withColumn("year", year(col("date"))) \
            .withColumn("month", month(col("date"))) \
            .withColumn("day", dayofmonth(col("date")))
        
        # Chemin de sortie
        historic_silver_path = f"{self.historic_path}/silver"
        
        # Écrire avec partitionnement
        write_mode = daily_summary.write.format("delta").mode("overwrite")
        
        if date_partition:
            write_mode = write_mode.partitionBy("year", "month", "day")
        
        write_mode.save(historic_silver_path)
        
        print(f"✅ Données Silver historisées (résumé quotidien): {historic_silver_path}")
        
        # Afficher un résumé
        historic_df = self.spark.read.format("delta").load(historic_silver_path)
        print(f"📊 Enregistrements historisés: {historic_df.count()}")
        print("📅 Agrégation: Résumé quotidien par symbole")
    
    def historize_gold_data(self):
        """
        Historise les données Gold (business metrics) avec snapshot quotidien
        """
        print("\n" + "="*60)
        print("🏆 HISTORISATION DES DONNÉES GOLD")
        print("="*60 + "\n")
        
        if not DeltaTable.isDeltaTable(self.spark, DELTA_GOLD_PATH):
            print(f"⚠️  Table Gold n'existe pas encore: {DELTA_GOLD_PATH}")
            return
        
        # Lire les données Gold
        gold_df = self.spark.read.format("delta").load(DELTA_GOLD_PATH)
        
        print(f"📊 Nombre d'enregistrements à historiser: {gold_df.count()}")
        
        # Ajouter la date du snapshot
        gold_with_date = gold_df \
            .withColumn("snapshot_date", date_format(current_timestamp(), "yyyy-MM-dd")) \
            .withColumn("year", year(col("snapshot_date"))) \
            .withColumn("month", month(col("snapshot_date"))) \
            .withColumn("day", dayofmonth(col("snapshot_date")))
        
        # Chemin de sortie
        historic_gold_path = f"{self.historic_path}/gold"
        
        # Écrire avec partitionnement par date
        gold_with_date.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .save(historic_gold_path)
        
        print(f"✅ Données Gold historisées (snapshot quotidien): {historic_gold_path}")
        
        # Afficher un résumé
        historic_df = self.spark.read.format("delta").load(historic_gold_path)
        print(f"📊 Enregistrements historisés: {historic_df.count()}")
        print("📅 Snapshot: État quotidien des métriques business")
    
    def create_daily_report(self):
        """
        Crée un rapport quotidien agrégé depuis les données historisées
        """
        print("\n" + "="*60)
        print("📊 CRÉATION DU RAPPORT QUOTIDIEN")
        print("="*60 + "\n")
        
        historic_silver_path = f"{self.historic_path}/silver"
        
        if not DeltaTable.isDeltaTable(self.spark, historic_silver_path):
            print("⚠️  Données historisées Silver non disponibles")
            return
        
        # Lire les données historisées
        historic_df = self.spark.read.format("delta").load(historic_silver_path)
        
        # Créer un rapport quotidien
        daily_report = historic_df \
            .groupBy("date") \
            .agg(
                count("symbol").alias("symbols_tracked"),
                spark_sum("daily_total_volume").alias("total_market_volume"),
                avg("daily_avg_volatility").alias("market_avg_volatility"),
                count("*").alias("total_records")
            ) \
            .orderBy(col("date").desc())
        
        print("📋 Rapport quotidien:")
        daily_report.show(truncate=False)
        
        # Sauvegarder le rapport
        report_path = f"{self.historic_path}/daily_reports"
        daily_report.write \
            .format("delta") \
            .mode("overwrite") \
            .save(report_path)
        
        print(f"✅ Rapport sauvegardé: {report_path}")
    
    def run_full_historization(self):
        """
        Exécute le job complet d'historisation
        """
        print("\n" + "="*70)
        print("🚀 DÉMARRAGE DU JOB BATCH D'HISTORISATION")
        print("="*70 + "\n")
        
        try:
            # Historiser chaque couche
            self.historize_bronze_data()
            self.historize_silver_data()
            self.historize_gold_data()
            
            # Créer le rapport quotidien
            self.create_daily_report()
            
            print("\n✅ Job batch d'historisation terminé avec succès!")
            print(f"📁 Données historisées disponibles dans: {self.historic_path}/")
            
        except Exception as e:
            print(f"\n❌ Erreur lors de l'historisation: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.spark.stop()
    
    def get_spark_session(self):
        """
        Retourne la session Spark
        """
        return self.spark

if __name__ == "__main__":
    job = BatchHistoricizationJob()
    job.run_full_historization()

