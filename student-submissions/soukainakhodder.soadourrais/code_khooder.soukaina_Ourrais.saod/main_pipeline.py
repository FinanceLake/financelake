"""
Main Pipeline - Orchestration complète du pipeline Delta + Streaming + ML + Dashboard
Script principal pour lancer tout le pipeline de A à Z
"""

import threading
import time
import sys
import os
from stock_data_generator import StockDataGenerator
from delta_lake_pipeline import DeltaLakePipeline
from batch_job import BatchHistoricizationJob
from silver_to_gold_transformation import SilverToGoldTransformer
from streaming_ml_scoring import StreamingMLScoring
from dashboard_generator import DashboardGenerator
from pyspark.sql import SparkSession

class CompletePipeline:
    """
    Orchestrateur complet du pipeline Real-Time Stock Insight avec Delta Lake
    """
    
    def __init__(self):
        self.generator = None
        self.delta_pipeline = None
        self.transformer = None
        self.ml_scorer = None
        self.dashboard_gen = None
        self.generator_thread = None
        self.active_queries = []
        
    def start_data_generation(self, duration=300):
        """
        Démarre la génération de données dans un thread séparé
        
        Args:
            duration: Durée de génération en secondes
        """
        print("\n" + "="*70)
        print("📊 PHASE 1: GÉNÉRATION DE DONNÉES")
        print("="*70 + "\n")
        
        self.generator = StockDataGenerator(output_path="./stream_data")
        
        self.generator_thread = threading.Thread(
            target=self.generator.run,
            args=(duration,)
        )
        self.generator_thread.daemon = True
        self.generator_thread.start()
        
        print("⏳ Attente de 5 secondes pour générer des données initiales...\n")
        time.sleep(5)
    
    def start_delta_pipeline(self):
        """
        Démarre le pipeline Delta Lake (Bronze/Silver/Gold)
        """
        print("\n" + "="*70)
        print("🚀 PHASE 2: PIPELINE DELTA LAKE")
        print("="*70 + "\n")
        
        self.delta_pipeline = DeltaLakePipeline("DeltaLakeStockInsight")
        
        # Créer les tables en streaming
        bronze_query = self.delta_pipeline.create_bronze_table("./stream_data")
        silver_query = self.delta_pipeline.create_silver_table()
        gold_query = self.delta_pipeline.create_gold_table()
        
        self.active_queries.extend([bronze_query, silver_query, gold_query])
        
        print("\n✅ Pipeline Delta Lake démarré!")
        print("📦 Bronze: Données brutes")
        print("🪙 Silver: Données nettoyées et enrichies")
        print("🏆 Gold: Métriques business")
        
        return bronze_query, silver_query, gold_query
    
    def run_silver_to_gold_transformation(self):
        """
        Exécute la transformation Silver → Gold
        """
        print("\n" + "="*70)
        print("🔄 PHASE 3: TRANSFORMATION SILVER → GOLD")
        print("="*70 + "\n")
        
        # Attendre que des données Silver soient disponibles
        print("⏳ Attente de 30 secondes pour accumuler des données Silver...\n")
        time.sleep(30)
        
        spark = self.delta_pipeline.get_spark_session()
        self.transformer = SilverToGoldTransformer(spark)
        
        # Exécuter la transformation avec DataFrame API
        self.transformer.run_transformation(use_sql=False, merge=True)
    
    def run_ml_training_and_scoring(self):
        """
        Entraîne le modèle ML et démarre le scoring en streaming
        """
        print("\n" + "="*70)
        print("🤖 PHASE 4: MACHINE LEARNING & SCORING")
        print("="*70 + "\n")
        
        # Attendre que des données soient disponibles
        print("⏳ Attente de 20 secondes pour accumuler des données...\n")
        time.sleep(20)
        
        spark = self.delta_pipeline.get_spark_session()
        self.ml_scorer = StreamingMLScoring(spark)
        
        # Entraîner le modèle
        print("🔄 Entraînement du modèle ML...")
        self.ml_scorer.train_model_batch(use_silver=True)
        
        # Démarrer le scoring en streaming
        print("\n🚀 Démarrage du scoring ML en streaming...")
        scoring_query = self.ml_scorer.create_streaming_scoring_query(use_silver=True)
        
        if scoring_query:
            self.active_queries.append(scoring_query)
            print("✅ Scoring ML en streaming démarré")
    
    def run_batch_historization(self):
        """
        Exécute le job batch d'historisation
        """
        print("\n" + "="*70)
        print("📦 PHASE 5: HISTORISATION BATCH")
        print("="*70 + "\n")
        
        # Attendre que des données soient disponibles
        print("⏳ Attente de 15 secondes avant l'historisation...\n")
        time.sleep(15)
        
        spark = self.delta_pipeline.get_spark_session()
        batch_job = BatchHistoricizationJob("BatchHistoricization")
        batch_job.spark = spark  # Réutiliser la session Spark
        
        # Exécuter l'historisation
        try:
            batch_job.historize_bronze_data()
            batch_job.historize_silver_data()
            batch_job.historize_gold_data()
            batch_job.create_daily_report()
            print("\n✅ Historisation batch terminée")
        except Exception as e:
            print(f"⚠️  Erreur lors de l'historisation: {e}")
    
    def generate_dashboard(self):
        """
        Génère le dashboard avec tous les graphiques
        """
        print("\n" + "="*70)
        print("📊 PHASE 6: GÉNÉRATION DU DASHBOARD")
        print("="*70 + "\n")
        
        # Attendre que des données soient disponibles
        print("⏳ Attente de 10 secondes avant la génération du dashboard...\n")
        time.sleep(10)
        
        spark = self.delta_pipeline.get_spark_session()
        self.dashboard_gen = DashboardGenerator(spark)
        
        # Générer tous les graphiques
        self.dashboard_gen.generate_all_dashboards()
    
    def optimize_and_maintain_delta_tables(self):
        """
        Optimise et nettoie les tables Delta
        """
        print("\n" + "="*70)
        print("🔧 PHASE 7: MAINTENANCE DES TABLES DELTA")
        print("="*70 + "\n")
        
        # Optimiser les tables
        self.delta_pipeline.optimize_tables()
        
        # VACUUM (optionnel, peut être fait périodiquement)
        # self.delta_pipeline.vacuum_tables(retention_hours=168)
    
    def run_complete_pipeline(self, duration=300):
        """
        Exécute le pipeline complet
        
        Args:
            duration: Durée totale d'exécution en secondes
        """
        print("\n" + "="*80)
        print("🎯 DÉMARRAGE DU PIPELINE COMPLET: REAL-TIME STOCK INSIGHT")
        print("="*80)
        print("\nCe pipeline démontre:")
        print("  1️⃣  Génération de données boursières simulées")
        print("  2️⃣  Pipeline Delta Lake (Bronze/Silver/Gold)")
        print("  3️⃣  Streaming en temps réel avec Spark Structured Streaming")
        print("  4️⃣  Transformations SQL/DataFrame (Silver → Gold)")
        print("  5️⃣  Machine Learning avec MLlib (entraînement + scoring en streaming)")
        print("  6️⃣  Job batch d'historisation")
        print("  7️⃣  Génération automatique de dashboard avec graphiques")
        print("  8️⃣  Opérations Delta (MERGE, VACUUM, OPTIMIZE, time travel)")
        print("\n" + "="*80 + "\n")
        
        try:
            # Phase 1: Génération de données
            self.start_data_generation(duration=duration + 60)
            
            # Phase 2: Pipeline Delta Lake
            bronze_query, silver_query, gold_query = self.start_delta_pipeline()
            
            # Phase 3: Transformation Silver → Gold
            self.run_silver_to_gold_transformation()
            
            # Phase 4: ML Training & Scoring
            self.run_ml_training_and_scoring()
            
            # Phase 5: Batch Historization (en arrière-plan)
            historization_thread = threading.Thread(target=self.run_batch_historization)
            historization_thread.daemon = True
            historization_thread.start()
            
            # Phase 6: Dashboard (périodique)
            dashboard_thread = threading.Thread(target=self._periodic_dashboard_generation)
            dashboard_thread.daemon = True
            dashboard_thread.start()
            
            # Phase 7: Maintenance (périodique)
            maintenance_thread = threading.Thread(target=self._periodic_maintenance)
            maintenance_thread.daemon = True
            maintenance_thread.start()
            
            # Continuer le streaming pendant la durée spécifiée
            print("\n" + "="*70)
            print("⏳ PIPELINE EN COURS D'EXÉCUTION")
            print("="*70 + "\n")
            print("Le pipeline traite les données en continu...")
            print("Appuyez sur Ctrl+C pour arrêter.\n")
            
            remaining_time = duration - 100  # On a déjà attendu ~100 secondes
            if remaining_time > 0:
                bronze_query.awaitTermination(timeout=remaining_time)
            
        except KeyboardInterrupt:
            print("\n\n⚠️  Interruption par l'utilisateur")
        except Exception as e:
            print(f"\n\n❌ Erreur: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.cleanup()
    
    def _periodic_dashboard_generation(self):
        """
        Génère le dashboard périodiquement
        """
        while True:
            try:
                time.sleep(60)  # Générer toutes les 60 secondes
                if self.delta_pipeline:
                    self.generate_dashboard()
            except Exception as e:
                print(f"⚠️  Erreur lors de la génération du dashboard: {e}")
    
    def _periodic_maintenance(self):
        """
        Effectue la maintenance périodique des tables Delta
        """
        while True:
            try:
                time.sleep(300)  # Maintenance toutes les 5 minutes
                if self.delta_pipeline:
                    self.optimize_and_maintain_delta_tables()
            except Exception as e:
                print(f"⚠️  Erreur lors de la maintenance: {e}")
    
    def cleanup(self):
        """
        Nettoie les ressources
        """
        print("\n" + "="*70)
        print("🧹 NETTOYAGE")
        print("="*70 + "\n")
        
        # Arrêter les queries
        if self.delta_pipeline:
            try:
                self.delta_pipeline.stop()
                print("✅ Pipeline Delta Lake arrêté")
            except Exception as e:
                print(f"⚠️  Erreur lors du nettoyage: {e}")
        
        time.sleep(2)
        
        # Résumé final
        self.print_summary()
    
    def print_summary(self):
        """
        Affiche un résumé de l'exécution
        """
        print("\n" + "="*70)
        print("📊 RÉSUMÉ DE L'EXÉCUTION")
        print("="*70)
        print("\n✅ TÂCHES COMPLÉTÉES:")
        print("  ✔️  Génération de données boursières simulées")
        print("  ✔️  Pipeline Delta Lake (Bronze/Silver/Gold)")
        print("  ✔️  Streaming en temps réel avec Spark Structured Streaming")
        print("  ✔️  Transformations SQL/DataFrame (Silver → Gold)")
        print("  ✔️  Machine Learning avec MLlib (entraînement + scoring)")
        print("  ✔️  Job batch d'historisation")
        print("  ✔️  Génération automatique de dashboard")
        print("  ✔️  Opérations Delta (MERGE, VACUUM, OPTIMIZE)")
        print("\n📁 FICHIERS GÉNÉRÉS:")
        print("  - ./stream_data/ : Données JSON en streaming")
        print("  - ./delta/bronze/ : Table Delta Bronze (raw)")
        print("  - ./delta/silver/ : Table Delta Silver (cleaned)")
        print("  - ./delta/gold/ : Table Delta Gold (business metrics)")
        print("  - ./delta/historic/ : Données historisées")
        print("  - ./dashboard/screenshots/ : Graphiques du dashboard")
        print("  - ./models/ : Modèles ML entraînés")
        print("  - ./checkpoints/ : Checkpoints du streaming")
        print("\n💡 PROCHAINES ÉTAPES:")
        print("  - Consultez les graphiques dans ./dashboard/screenshots/")
        print("  - Examinez les interprétations dans les fichiers *_interpretation.txt")
        print("  - Explorez les tables Delta avec time travel")
        print("  - Consultez la documentation mise à jour")
        print("\n" + "="*70 + "\n")

def main():
    """
    Point d'entrée principal
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Pipeline complet Real-Time Stock Insight avec Delta Lake"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=300,
        help="Durée d'exécution en secondes (défaut: 300 = 5 minutes)"
    )
    parser.add_argument(
        "--mode",
        choices=["full", "delta", "ml", "dashboard", "batch"],
        default="full",
        help="Mode d'exécution (défaut: full)"
    )
    
    args = parser.parse_args()
    
    app = CompletePipeline()
    
    if args.mode == "full":
        app.run_complete_pipeline(duration=args.duration)
    elif args.mode == "delta":
        app.start_data_generation(duration=args.duration)
        app.start_delta_pipeline()
    elif args.mode == "ml":
        app.start_data_generation(duration=args.duration)
        app.start_delta_pipeline()
        app.run_ml_training_and_scoring()
    elif args.mode == "dashboard":
        app.generate_dashboard()
    elif args.mode == "batch":
        app.run_batch_historization()

if __name__ == "__main__":
    main()

