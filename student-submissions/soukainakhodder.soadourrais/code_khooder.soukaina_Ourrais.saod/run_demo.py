"""
Script de Démonstration Complète
Execute le pipeline avec génération automatique de toutes les analyses
"""

import time
import threading
from stock_data_generator import StockDataGenerator
from spark_streaming_pipeline import StockStreamingPipeline
from spark_sql_analysis import SparkSQLAnalyzer
from spark_mllib_model import StockPricePredictor
from visualization import StockVisualizer

def run_complete_demo():
    """
    Exécute une démonstration complète du pipeline
    """
    print("""
========================================================================
                                                                      
       REAL-TIME STOCK INSIGHT - DEMONSTRATION COMPLETE        
                                                                      
                      avec Apache Spark                               
                                                                      
========================================================================

Ce script demontre:
  - Spark Structured Streaming avec agregations fenetrees
  - Spark SQL avec optimisation cache et Catalyst
  - Spark MLlib avec Regression Logistique et Random Forest
  - Visualisations avancees et indicateur RSI

Duree estimee: 2-3 minutes
""")
    
    input("Appuyez sur ENTRÉE pour commencer...")
    
    # Variables globales
    generator = None
    pipeline = None
    
    try:
        # ====================================================================
        # PHASE 1: Génération de données
        # ====================================================================
        print("\n" + "="*70)
        print("[PHASE 1/5] GENERATION DE DONNEES BOURSIERES")
        print("="*70)
        
        generator = StockDataGenerator(output_path="./stream_data")
        
        # Lancer le générateur dans un thread
        generator_thread = threading.Thread(
            target=generator.run,
            args=(180,)  # 3 minutes
        )
        generator_thread.daemon = True
        generator_thread.start()
        
        print("\n⏳ Attente de 5 secondes pour générer les premières données...\n")
        time.sleep(5)
        
        # ====================================================================
        # PHASE 2: Pipeline Spark Streaming
        # ====================================================================
        print("\n" + "="*70)
        print("🚀 PHASE 2/5: DÉMARRAGE DU PIPELINE STREAMING")
        print("="*70)
        
        pipeline = StockStreamingPipeline("RealTimeStockInsightDemo")
        
        # Créer le streaming DataFrame
        streaming_df = pipeline.create_streaming_dataframe("./stream_data")
        
        # Appliquer les agrégations
        aggregated_df = pipeline.apply_windowed_aggregations(streaming_df)
        
        # Écrire vers mémoire et Parquet
        memory_query = pipeline.write_to_memory(aggregated_df, "stock_aggregates")
        parquet_query = pipeline.write_to_parquet(streaming_df, "./output/stock_data")
        
        print("\n✅ Pipeline streaming actif!")
        print("⏳ Attente de 25 secondes pour accumuler des données...\n")
        time.sleep(25)
        
        # ====================================================================
        # PHASE 3: Analyse Spark SQL
        # ====================================================================
        print("\n" + "="*70)
        print("[PHASE 3/5] ANALYSE SPARK SQL")
        print("="*70)
        
        spark = pipeline.get_spark_session()
        analyzer = SparkSQLAnalyzer(spark)
        
        # Créer la vue temporaire
        analyzer.create_temp_view("stock_aggregates", "stock_view")
        
        print("\n🔍 Exécution des requêtes SQL...\n")
        
        # Requêtes
        analyzer.query_average_price_by_symbol()
        analyzer.query_top_volatile_stocks(top_n=5)
        analyzer.query_high_volume_periods(volume_threshold=500000)
        
        # Comparaison cache
        print("\n" + "-"*70)
        analyzer.compare_cache_performance()
        
        # Plan d'exécution
        print("\n" + "-"*70)
        analyzer.analyze_execution_plan()
        
        # ====================================================================
        # PHASE 4: Machine Learning avec MLlib
        # ====================================================================
        print("\n" + "="*70)
        print("🤖 PHASE 4/5: ENTRAÎNEMENT DES MODÈLES MLLIB")
        print("="*70)
        
        print("\n⏳ Attente de 15 secondes pour collecter des données d'entraînement...\n")
        time.sleep(15)
        
        predictor = StockPricePredictor(spark)
        ml_results = predictor.run_full_training()
        
        # ====================================================================
        # PHASE 5: Visualisations
        # ====================================================================
        print("\n" + "="*70)
        print("📈 PHASE 5/5: GÉNÉRATION DES VISUALISATIONS")
        print("="*70)
        
        visualizer = StockVisualizer(spark)
        visualizer.generate_all_visualizations()
        
        # ====================================================================
        # Résumé Final
        # ====================================================================
        print("\n" + "="*70)
        print(">>> DEMONSTRATION TERMINEE AVEC SUCCES!")
        print("="*70)
        
        print("""
========================================================================
                      RESUME DES RESULTATS                         
========================================================================

[OK] TACHES COMPLETEES:

  [1] Ingestion Temps Reel (Spark Streaming)
      - Flux JSON parse avec schema explicite
      - Agregations par fenetres glissantes (10s)
      - Metriques: prix moyen, volatilite, volume

  [2] Analyse avec Spark SQL
      - Vues temporaires creees
      - Requetes SQL complexes executees
      - Comparaison performances cache vs no-cache
      - Plan d'execution analyse (Catalyst Optimizer)

  [3] Modelisation MLlib
      - Features engineering (lag, temporal)
      - Regression Logistique entrainee
      - Random Forest entraine
      - Metriques: AUC, Accuracy, Precision, Recall, F1

  [4] Visualisations et Extensions
      - Graphiques generes dans ./screenshots/
      - Extension: RSI (Relative Strength Index)

[FICHIERS] FICHIERS GENERES:

  ./stream_data/         - Donnees JSON en streaming
  ./output/stock_data/   - Donnees Parquet pour batch
  ./checkpoints/         - Checkpoints Spark
  ./screenshots/         - Graphiques de visualisation

[DOCS] DOCUMENTATION:

  README.md              - Guide complet d'utilisation
  RAPPORT.md             - Mini-rapport (400 mots)

[NEXT] PROCHAINES ETAPES:

  1. Consultez les graphiques dans ./screenshots/
  2. Lisez le RAPPORT.md pour comprendre l'architecture
  3. Explorez le code source avec les commentaires detailles
  4. Testez differentes configurations dans config.py

========================================================================
              TOUTES LES EXIGENCES DU LAB SATISFAITES          
========================================================================
""")
        
        print("\n[INFO] Le streaming continue pendant encore 30 secondes...")
        print("       (Vous pouvez interrompre avec Ctrl+C)\n")
        
        time.sleep(30)
        
    except KeyboardInterrupt:
        print("\n\n[WARN] Interruption par l'utilisateur")
    except Exception as e:
        print(f"\n\n[ERROR] Erreur: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Nettoyage
        if pipeline:
            print("\n🧹 Arrêt du pipeline...")
            try:
                pipeline.stop()
            except Exception as e:
                print(f"⚠️  Erreur lors du nettoyage (peut être ignorée): {e}")
        
        print("\n✅ Démonstration terminée!\n")

if __name__ == "__main__":
    run_complete_demo()

