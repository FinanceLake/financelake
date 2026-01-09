"""
Script Principal - Intégration Complète du Pipeline
Exécute le pipeline complet: Streaming + SQL + MLlib
"""

import threading
import time
import sys
from stock_data_generator import StockDataGenerator
from spark_streaming_pipeline import StockStreamingPipeline
from spark_sql_analysis import SparkSQLAnalyzer
from spark_mllib_model import StockPricePredictor

class RealTimeStockInsight:
    """
    Orchestrateur principal du pipeline Real-Time Stock Insight
    """
    
    def __init__(self):
        self.generator = None
        self.pipeline = None
        self.analyzer = None
        self.predictor = None
        self.generator_thread = None
        
    def start_data_generation(self, duration=180):
        """
        Démarre la génération de données dans un thread séparé
        
        Args:
            duration: Durée de génération en secondes
        """
        print("\n" + "="*70)
        print("📊 PHASE 1: GÉNÉRATION DE DONNÉES")
        print("="*70 + "\n")
        
        self.generator = StockDataGenerator(output_path="./stream_data")
        
        # Lancer dans un thread séparé
        self.generator_thread = threading.Thread(
            target=self.generator.run,
            args=(duration,)
        )
        self.generator_thread.daemon = True
        self.generator_thread.start()
        
        # Attendre que quelques fichiers soient générés
        print("⏳ Attente de 5 secondes pour générer des données initiales...\n")
        time.sleep(5)
    
    def start_streaming_pipeline(self):
        """
        Démarre le pipeline Spark Streaming
        """
        print("\n" + "="*70)
        print("🚀 PHASE 2: STREAMING PIPELINE")
        print("="*70 + "\n")
        
        self.pipeline = StockStreamingPipeline("RealTimeStockInsight")
        
        # Créer le DataFrame en streaming
        streaming_df = self.pipeline.create_streaming_dataframe("./stream_data")
        
        # Appliquer les agrégations
        aggregated_df = self.pipeline.apply_windowed_aggregations(streaming_df)
        
        # Écrire vers la mémoire et la console
        memory_query = self.pipeline.write_to_memory(aggregated_df, "stock_aggregates")
        
        # Aussi écrire vers Parquet pour l'entraînement MLlib
        parquet_query = self.pipeline.write_to_parquet(streaming_df, "./output/stock_data")
        
        return memory_query, parquet_query
    
    def run_sql_analysis(self):
        """
        Exécute l'analyse Spark SQL
        """
        print("\n" + "="*70)
        print("📊 PHASE 3: ANALYSE SPARK SQL")
        print("="*70 + "\n")
        
        # Attendre que des données s'accumulent
        print("⏳ Attente de 20 secondes pour accumuler des données...\n")
        time.sleep(20)
        
        spark = self.pipeline.get_spark_session()
        self.analyzer = SparkSQLAnalyzer(spark)
        self.analyzer.run_all_analyses("stock_aggregates")
    
    def run_ml_training(self):
        """
        Exécute l'entraînement MLlib
        """
        print("\n" + "="*70)
        print("🤖 PHASE 4: ENTRAÎNEMENT MLLIB")
        print("="*70 + "\n")
        
        # Attendre que suffisamment de données soient écrites
        print("⏳ Attente de 15 secondes pour collecter des données d'entraînement...\n")
        time.sleep(15)
        
        spark = self.pipeline.get_spark_session()
        self.predictor = StockPricePredictor(spark)
        results = self.predictor.run_full_training()
        
        return results
    
    def run_complete_pipeline(self, duration=120):
        """
        Exécute le pipeline complet
        
        Args:
            duration: Durée totale d'exécution en secondes
        """
        print("\n" + "="*80)
        print("🎯 DÉMARRAGE DU PIPELINE COMPLET: REAL-TIME STOCK INSIGHT")
        print("="*80)
        print("\nCe pipeline démontre:")
        print("  1️⃣  Streaming en temps réel avec Spark Structured Streaming")
        print("  2️⃣  Agrégations par fenêtres temporelles")
        print("  3️⃣  Analyse SQL avec cache et optimisation Catalyst")
        print("  4️⃣  Modélisation prédictive avec MLlib (LR + Random Forest)")
        print("\n" + "="*80 + "\n")
        
        try:
            # Phase 1: Génération de données
            self.start_data_generation(duration=duration + 60)
            
            # Phase 2: Streaming
            memory_query, parquet_query = self.start_streaming_pipeline()
            
            # Phase 3: Analyse SQL
            self.run_sql_analysis()
            
            # Phase 4: Machine Learning
            ml_results = self.run_ml_training()
            
            # Continuer le streaming pendant un moment
            print("\n" + "="*70)
            print("⏳ PHASE 5: MONITORING CONTINU")
            print("="*70 + "\n")
            print("Le pipeline continue à traiter les données...")
            print("Appuyez sur Ctrl+C pour arrêter.\n")
            
            # Requêtes périodiques pendant que le streaming continue
            remaining_time = duration - 60  # On a déjà attendu ~60 secondes
            if remaining_time > 0:
                memory_query.awaitTermination(timeout=remaining_time)
            
        except KeyboardInterrupt:
            print("\n\n⚠️  Interruption par l'utilisateur")
        except Exception as e:
            print(f"\n\n❌ Erreur: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.cleanup()
    
    def cleanup(self):
        """
        Nettoie les ressources
        """
        import time
        
        print("\n" + "="*70)
        print("🧹 NETTOYAGE")
        print("="*70 + "\n")
        
        if self.pipeline:
            try:
                self.pipeline.stop()
                print("✅ Pipeline arrêté proprement")
            except Exception as e:
                print(f"⚠️  Erreur lors du nettoyage (peut être ignorée): {e}")
        
        # Attendre que tous les processus se terminent proprement
        time.sleep(1)
        
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
        print("  ✔️  Tâche 1: Ingestion temps réel avec Spark Streaming")
        print("      - Agrégations par fenêtres temporelles (10s)")
        print("      - Calcul de volatilité et statistiques")
        print("\n  ✔️  Tâche 2: Analyse avec Spark SQL")
        print("      - Requêtes SQL sur vues temporaires")
        print("      - Comparaison avec/sans cache")
        print("      - Analyse du plan d'exécution (Catalyst)")
        print("\n  ✔️  Tâche 3: Modélisation MLlib")
        print("      - Régression Logistique")
        print("      - Random Forest")
        print("      - Prédiction d'augmentation de prix")
        print("\n📁 FICHIERS GÉNÉRÉS:")
        print("  - ./stream_data/ : Données JSON en streaming")
        print("  - ./output/stock_data/ : Données Parquet pour analyse batch")
        print("  - ./checkpoints/ : Checkpoints du streaming")
        print("\n💡 PROCHAINES ÉTAPES:")
        print("  - Consultez le RAPPORT.md pour l'analyse détaillée")
        print("  - Examinez les captures d'écran dans ./screenshots/")
        print("  - Explorez les visualisations dans visualization.py")
        print("\n" + "="*70 + "\n")

def main():
    """
    Point d'entrée principal
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Pipeline Real-Time Stock Insight avec Apache Spark"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=120,
        help="Durée d'exécution en secondes (défaut: 120)"
    )
    parser.add_argument(
        "--mode",
        choices=["full", "streaming", "sql", "ml"],
        default="full",
        help="Mode d'exécution (défaut: full)"
    )
    
    args = parser.parse_args()
    
    app = RealTimeStockInsight()
    
    if args.mode == "full":
        app.run_complete_pipeline(duration=args.duration)
    elif args.mode == "streaming":
        app.start_data_generation(duration=args.duration)
        app.start_streaming_pipeline()
    elif args.mode == "sql":
        # Nécessite que le streaming soit déjà actif
        print("⚠️  Assurez-vous que le streaming est actif dans un autre terminal")
        app.run_sql_analysis()
    elif args.mode == "ml":
        app.run_ml_training()

if __name__ == "__main__":
    main()

