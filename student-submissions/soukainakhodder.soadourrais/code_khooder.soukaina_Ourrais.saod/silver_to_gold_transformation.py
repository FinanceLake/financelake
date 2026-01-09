"""
Silver to Gold Transformation
Transformations SQL/DataFrame pour passer de Silver à Gold
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, sum as spark_sum, count, max as spark_max, min as spark_min,
    when, current_timestamp, window, stddev
)
from delta.tables import DeltaTable
from config import DELTA_SILVER_PATH, DELTA_GOLD_PATH

class SilverToGoldTransformer:
    """
    Transforme les données Silver en données Gold avec métriques business
    """
    
    def __init__(self, spark_session):
        """
        Initialise le transformateur avec une session Spark
        
        Args:
            spark_session: Session Spark active
        """
        self.spark = spark_session
        print("✅ Transformateur Silver → Gold initialisé")
    
    def transform_with_dataframe(self):
        """
        Transformation Silver → Gold en utilisant l'API DataFrame
        """
        print("\n" + "="*60)
        print("🔄 TRANSFORMATION SILVER → GOLD (DataFrame API)")
        print("="*60 + "\n")
        
        if not DeltaTable.isDeltaTable(self.spark, DELTA_SILVER_PATH):
            print(f"⚠️  Table Silver n'existe pas: {DELTA_SILVER_PATH}")
            return None
        
        # Lire les données Silver
        silver_df = self.spark.read.format("delta").load(DELTA_SILVER_PATH)
        
        print(f"📊 Enregistrements Silver: {silver_df.count()}")
        
        # Transformation: Agréger par symbole pour créer des métriques business
        gold_df = silver_df \
            .groupBy("symbol") \
            .agg(
                # Métriques de prix
                avg("avg_price").alias("overall_avg_price"),
                spark_min("min_price").alias("absolute_min_price"),
                spark_max("max_price").alias("absolute_max_price"),
                
                # Métriques de volatilité
                avg("volatility").alias("overall_volatility"),
                stddev("volatility").alias("volatility_stddev"),
                
                # Métriques de volume
                spark_sum("total_volume").alias("cumulative_volume"),
                avg("total_volume").alias("avg_volume_per_window"),
                
                # Métriques de transactions
                spark_sum("transaction_count").alias("total_transactions"),
                avg("transaction_count").alias("avg_transactions_per_window"),
                
                # Métriques de tendance
                avg("avg_price_change").alias("trend_direction"),
                count("*").alias("window_count"),
                
                # Timestamps
                spark_max("window_start").alias("last_window_time"),
                spark_max("last_updated").alias("last_silver_update")
            ) \
            .withColumn(
                "price_trend",
                when(col("trend_direction") > 0.1, "STRONG_UP")
                .when(col("trend_direction") > 0, "UP")
                .when(col("trend_direction") < -0.1, "STRONG_DOWN")
                .when(col("trend_direction") < 0, "DOWN")
                .otherwise("STABLE")
            ) \
            .withColumn(
                "volatility_category",
                when(col("overall_volatility") > 5, "HIGH")
                .when(col("overall_volatility") > 2, "MEDIUM")
                .otherwise("LOW")
            ) \
            .withColumn(
                "volume_category",
                when(col("cumulative_volume") > 10000000, "HIGH")
                .when(col("cumulative_volume") > 5000000, "MEDIUM")
                .otherwise("LOW")
            ) \
            .withColumn(
                "price_range",
                col("absolute_max_price") - col("absolute_min_price")
            ) \
            .withColumn(
                "price_range_percent",
                (col("price_range") / col("overall_avg_price")) * 100
            ) \
            .withColumn("gold_ingestion_time", current_timestamp())
        
        print("✅ Transformation DataFrame terminée")
        print("📊 Colonnes créées:")
        gold_df.printSchema()
        
        return gold_df
    
    def transform_with_sql(self):
        """
        Transformation Silver → Gold en utilisant Spark SQL
        """
        print("\n" + "="*60)
        print("🔄 TRANSFORMATION SILVER → GOLD (Spark SQL)")
        print("="*60 + "\n")
        
        if not DeltaTable.isDeltaTable(self.spark, DELTA_SILVER_PATH):
            print(f"⚠️  Table Silver n'existe pas: {DELTA_SILVER_PATH}")
            return None
        
        # Créer une vue temporaire depuis Silver
        silver_df = self.spark.read.format("delta").load(DELTA_SILVER_PATH)
        silver_df.createOrReplaceTempView("silver_stock_data")
        
        # Requête SQL pour transformation
        sql_query = """
        SELECT 
            symbol,
            
            -- Métriques de prix
            AVG(avg_price) as overall_avg_price,
            MIN(min_price) as absolute_min_price,
            MAX(max_price) as absolute_max_price,
            MAX(max_price) - MIN(min_price) as price_range,
            ((MAX(max_price) - MIN(min_price)) / AVG(avg_price)) * 100 as price_range_percent,
            
            -- Métriques de volatilité
            AVG(volatility) as overall_volatility,
            STDDEV(volatility) as volatility_stddev,
            CASE 
                WHEN AVG(volatility) > 5 THEN 'HIGH'
                WHEN AVG(volatility) > 2 THEN 'MEDIUM'
                ELSE 'LOW'
            END as volatility_category,
            
            -- Métriques de volume
            SUM(total_volume) as cumulative_volume,
            AVG(total_volume) as avg_volume_per_window,
            CASE 
                WHEN SUM(total_volume) > 10000000 THEN 'HIGH'
                WHEN SUM(total_volume) > 5000000 THEN 'MEDIUM'
                ELSE 'LOW'
            END as volume_category,
            
            -- Métriques de transactions
            SUM(transaction_count) as total_transactions,
            AVG(transaction_count) as avg_transactions_per_window,
            
            -- Métriques de tendance
            AVG(avg_price_change) as trend_direction,
            CASE 
                WHEN AVG(avg_price_change) > 0.1 THEN 'STRONG_UP'
                WHEN AVG(avg_price_change) > 0 THEN 'UP'
                WHEN AVG(avg_price_change) < -0.1 THEN 'STRONG_DOWN'
                WHEN AVG(avg_price_change) < 0 THEN 'DOWN'
                ELSE 'STABLE'
            END as price_trend,
            
            -- Compteurs
            COUNT(*) as window_count,
            
            -- Timestamps
            MAX(window_start) as last_window_time,
            MAX(last_updated) as last_silver_update,
            CURRENT_TIMESTAMP() as gold_ingestion_time
            
        FROM silver_stock_data
        GROUP BY symbol
        ORDER BY cumulative_volume DESC
        """
        
        print("📋 Requête SQL:")
        print(sql_query)
        print()
        
        gold_df = self.spark.sql(sql_query)
        
        print("✅ Transformation SQL terminée")
        print("📊 Résultats:")
        gold_df.show(truncate=False)
        
        return gold_df
    
    def write_to_gold(self, gold_df, merge=True):
        """
        Écrit les données transformées vers la table Gold
        
        Args:
            gold_df: DataFrame Gold à écrire
            merge: Si True, utilise MERGE pour éviter les doublons
        """
        print("\n" + "="*60)
        print("💾 ÉCRITURE VERS LA TABLE GOLD")
        print("="*60 + "\n")
        
        if gold_df is None:
            print("❌ Aucune donnée à écrire")
            return
        
        if merge and DeltaTable.isDeltaTable(self.spark, DELTA_GOLD_PATH):
            # Utiliser MERGE pour mettre à jour ou insérer
            print("🔄 Utilisation de MERGE pour mettre à jour la table Gold...")
            
            delta_table = DeltaTable.forPath(self.spark, DELTA_GOLD_PATH)
            
            delta_table.alias("target").merge(
                gold_df.alias("source"),
                "target.symbol = source.symbol"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            
            print("✅ Table Gold mise à jour avec MERGE")
        else:
            # Créer ou écraser la table
            print("📝 Création/écrasement de la table Gold...")
            
            gold_df.write \
                .format("delta") \
                .mode("overwrite" if not merge else "append") \
                .save(DELTA_GOLD_PATH)
            
            print("✅ Table Gold créée/mise à jour")
        
        # Afficher le résultat
        result_df = self.spark.read.format("delta").load(DELTA_GOLD_PATH)
        print(f"\n📊 Enregistrements dans Gold: {result_df.count()}")
        result_df.show(truncate=False)
    
    def run_transformation(self, use_sql=False, merge=True):
        """
        Exécute la transformation complète Silver → Gold
        
        Args:
            use_sql: Si True, utilise SQL au lieu de DataFrame API
            merge: Si True, utilise MERGE pour écrire vers Gold
        """
        print("\n" + "="*70)
        print("🚀 TRANSFORMATION COMPLÈTE SILVER → GOLD")
        print("="*70 + "\n")
        
        try:
            # Transformer selon la méthode choisie
            if use_sql:
                gold_df = self.transform_with_sql()
            else:
                gold_df = self.transform_with_dataframe()
            
            # Écrire vers Gold
            if gold_df is not None:
                self.write_to_gold(gold_df, merge=merge)
                print("\n✅ Transformation complète terminée!")
            else:
                print("\n⚠️  Aucune transformation effectuée")
                
        except Exception as e:
            print(f"\n❌ Erreur lors de la transformation: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("SilverToGoldTransformation") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    transformer = SilverToGoldTransformer(spark)
    
    # Tester avec DataFrame API
    transformer.run_transformation(use_sql=False, merge=True)
    
    # Tester avec SQL
    # transformer.run_transformation(use_sql=True, merge=True)
    
    spark.stop()

