"""
Spark SQL Analysis - Tâche 2
Analyse des données avec Spark SQL, cache, et analyse du plan d'exécution
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, count, desc
import time

class SparkSQLAnalyzer:
    """
    Analyseur utilisant Spark SQL pour interroger les données agrégées
    """
    
    def __init__(self, spark_session):
        """
        Initialise l'analyseur avec une session Spark existante
        
        Args:
            spark_session: Session Spark active
        """
        self.spark = spark_session
        print("✅ Analyseur Spark SQL initialisé")
    
    def create_temp_view(self, table_name="stock_aggregates", view_name="stock_view"):
        """
        Crée une vue temporaire à partir de la table en mémoire
        
        Args:
            table_name: Nom de la table source
            view_name: Nom de la vue temporaire à créer
        """
        try:
            df = self.spark.table(table_name)
            df.createOrReplaceTempView(view_name)
            print(f"📊 Vue temporaire '{view_name}' créée depuis '{table_name}'")
            return True
        except Exception as e:
            print(f"❌ Erreur lors de la création de la vue: {e}")
            return False
    
    def query_average_price_by_symbol(self, view_name="stock_view"):
        """
        Requête SQL: Prix moyen et volatilité moyenne par symbole
        
        Returns:
            DataFrame avec résultats
        """
        query = f"""
        SELECT 
            symbol,
            AVG(avg_price) as overall_avg_price,
            AVG(volatility) as overall_volatility,
            SUM(total_volume) as cumulative_volume,
            COUNT(*) as window_count,
            MIN(min_price) as absolute_min_price,
            MAX(max_price) as absolute_max_price
        FROM {view_name}
        GROUP BY symbol
        ORDER BY overall_avg_price DESC
        """
        
        print("\n" + "="*60)
        print("📊 REQUÊTE SQL: Statistiques par Symbole")
        print("="*60)
        print(query)
        
        result_df = self.spark.sql(query)
        result_df.show(truncate=False)
        
        return result_df
    
    def query_top_volatile_stocks(self, view_name="stock_view", top_n=5):
        """
        Requête SQL: Trouver les actions les plus volatiles
        
        Args:
            view_name: Nom de la vue temporaire
            top_n: Nombre de résultats à retourner
            
        Returns:
            DataFrame avec les actions les plus volatiles
        """
        query = f"""
        SELECT 
            symbol,
            AVG(volatility) as avg_volatility,
            MAX(volatility) as max_volatility,
            AVG(avg_price_change) as avg_price_change_pct
        FROM {view_name}
        WHERE volatility IS NOT NULL
        GROUP BY symbol
        ORDER BY avg_volatility DESC
        LIMIT {top_n}
        """
        
        print("\n" + "="*60)
        print(f"📈 REQUÊTE SQL: Top {top_n} Actions les Plus Volatiles")
        print("="*60)
        print(query)
        
        result_df = self.spark.sql(query)
        result_df.show(truncate=False)
        
        return result_df
    
    def query_high_volume_periods(self, view_name="stock_view", volume_threshold=1000000):
        """
        Requête SQL: Périodes de volume élevé
        
        Args:
            view_name: Nom de la vue temporaire
            volume_threshold: Seuil de volume
            
        Returns:
            DataFrame avec les périodes de volume élevé
        """
        query = f"""
        SELECT 
            window_start,
            window_end,
            symbol,
            total_volume,
            avg_price,
            transaction_count
        FROM {view_name}
        WHERE total_volume > {volume_threshold}
        ORDER BY total_volume DESC
        """
        
        print("\n" + "="*60)
        print(f"📊 REQUÊTE SQL: Périodes de Volume Élevé (> {volume_threshold:,})")
        print("="*60)
        print(query)
        
        result_df = self.spark.sql(query)
        result_df.show(truncate=False)
        
        return result_df
    
    def compare_cache_performance(self, view_name="stock_view"):
        """
        Compare les performances avec et sans cache
        
        Cette fonction démontre l'impact du cache sur les performances
        en exécutant la même requête deux fois: une fois sans cache,
        et une fois avec cache.
        """
        print("\n" + "="*60)
        print("🔬 ANALYSE DE PERFORMANCE: Cache vs No Cache")
        print("="*60 + "\n")
        
        # Requête complexe pour tester
        complex_query = f"""
        SELECT 
            symbol,
            AVG(avg_price) as avg_price,
            STDDEV(avg_price) as price_std,
            AVG(volatility) as avg_volatility,
            SUM(total_volume) as total_volume
        FROM {view_name}
        GROUP BY symbol
        ORDER BY avg_volatility DESC
        """
        
        # === Test SANS cache ===
        print("1️⃣  Exécution SANS cache...")
        df_no_cache = self.spark.sql(complex_query)
        
        start_time = time.time()
        df_no_cache.show()
        no_cache_time = time.time() - start_time
        
        print(f"⏱️  Temps sans cache: {no_cache_time:.4f} secondes\n")
        
        # === Test AVEC cache ===
        print("2️⃣  Exécution AVEC cache...")
        df_with_cache = self.spark.sql(complex_query)
        df_with_cache.cache()  # Mise en cache
        
        # Première exécution (remplit le cache)
        start_time = time.time()
        df_with_cache.show()
        first_cache_time = time.time() - start_time
        
        print(f"⏱️  Temps avec cache (1ère exécution): {first_cache_time:.4f} secondes")
        
        # Deuxième exécution (utilise le cache)
        start_time = time.time()
        df_with_cache.show()
        second_cache_time = time.time() - start_time
        
        print(f"⏱️  Temps avec cache (2ème exécution): {second_cache_time:.4f} secondes\n")
        
        # Résumé
        print("📊 RÉSUMÉ:")
        print(f"   - Sans cache: {no_cache_time:.4f}s")
        print(f"   - Avec cache (1ère): {first_cache_time:.4f}s")
        print(f"   - Avec cache (2ème): {second_cache_time:.4f}s")
        
        if second_cache_time < no_cache_time:
            speedup = no_cache_time / second_cache_time
            print(f"   - Accélération: {speedup:.2f}x plus rapide avec cache! 🚀")
        
        # Nettoyer le cache
        df_with_cache.unpersist()
        
        return {
            "no_cache": no_cache_time,
            "first_cache": first_cache_time,
            "second_cache": second_cache_time
        }
    
    def analyze_execution_plan(self, view_name="stock_view"):
        """
        Analyse le plan d'exécution avec explain('formatted')
        
        Permet d'observer l'impact du Catalyst Optimizer
        """
        print("\n" + "="*60)
        print("🔍 ANALYSE DU PLAN D'EXÉCUTION (Catalyst Optimizer)")
        print("="*60 + "\n")
        
        query = f"""
        SELECT 
            symbol,
            AVG(avg_price) as avg_price,
            AVG(volatility) as avg_volatility
        FROM {view_name}
        WHERE volatility > 0
        GROUP BY symbol
        """
        
        df = self.spark.sql(query)
        
        print("📋 Plan d'exécution PHYSIQUE:")
        print("-" * 60)
        df.explain(mode='formatted')
        
        print("\n📋 Plan d'exécution SIMPLE:")
        print("-" * 60)
        df.explain(mode='simple')
        
        print("\n📋 Plan d'exécution ÉTENDU:")
        print("-" * 60)
        df.explain(mode='extended')
        
        print("\n📋 Plan d'exécution AVEC COÛT:")
        print("-" * 60)
        df.explain(mode='cost')
        
    def run_all_analyses(self, table_name="stock_aggregates"):
        """
        Exécute toutes les analyses SQL
        
        Args:
            table_name: Nom de la table à analyser
        """
        print("\n" + "="*70)
        print("🚀 DÉMARRAGE DE L'ANALYSE SPARK SQL COMPLÈTE")
        print("="*70)
        
        # Créer la vue temporaire
        if not self.create_temp_view(table_name, "stock_view"):
            print("❌ Impossible de créer la vue. Assurez-vous que le streaming est actif.")
            return
        
        # Attendre un peu pour accumuler des données
        print("\n⏳ Attente de 10 secondes pour accumuler des données...\n")
        time.sleep(10)
        
        # Exécuter les analyses
        self.query_average_price_by_symbol()
        self.query_top_volatile_stocks()
        self.query_high_volume_periods(volume_threshold=500000)
        
        # Analyser les performances du cache
        self.compare_cache_performance()
        
        # Analyser le plan d'exécution
        self.analyze_execution_plan()
        
        print("\n✅ Analyse Spark SQL terminée!")

if __name__ == "__main__":
    # Ce module doit être utilisé avec un pipeline de streaming actif
    print("⚠️  Ce module doit être utilisé avec spark_streaming_pipeline.py")
    print("    Utilisez main.py pour exécuter le pipeline complet.")

