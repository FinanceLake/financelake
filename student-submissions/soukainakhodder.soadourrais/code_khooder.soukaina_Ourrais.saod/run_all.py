"""
Script Principal - Lancement complet du pipeline de A à Z
Ce script orchestre toutes les phases du projet selon les exigences du professeur:
1. Delta Lake (Bronze/Silver/Gold)
2. Pipeline batch + streaming + SQL
3. MLlib dans le streaming
4. Dashboard automatique
5. Historisation batch
"""

import sys
import os

# Ajouter le répertoire courant au path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main_pipeline import CompletePipeline

def main():
    """
    Point d'entrée principal - Lance le pipeline complet
    """
    print("""
    ╔══════════════════════════════════════════════════════════════════════╗
    ║                                                                      ║
    ║         REAL-TIME STOCK INSIGHT - PIPELINE COMPLET                   ║
    ║                                                                      ║
    ║         Conforme aux exigences du professeur                         ║
    ║                                                                      ║
    ╚══════════════════════════════════════════════════════════════════════╝
    
    Ce pipeline exécute:
    
    ✅ 1. DELTA LAKE
       • Tables Bronze, Silver, Gold en Delta
       • Opérations: MERGE, VACUUM, OPTIMIZE, Time Travel
       • Schéma: ./delta/bronze/, ./delta/silver/, ./delta/gold/
    
    ✅ 2. PIPELINE BATCH + STREAMING + SQL
       • Streaming en temps réel
       • Job batch d'historisation
       • Transformations SQL/DataFrame (Silver → Gold)
    
    ✅ 3. MLLIB DANS LE STREAMING
       • Lecture depuis Silver/Gold
       • Calcul de features en temps réel
       • Entraînement (RandomForest/LogisticRegression)
       • Real-Time Scoring sur flux
    
    ✅ 4. DASHBOARD FONCTIONNEL
       • Génération automatique de graphiques
       • Sauvegarde dans ./dashboard/screenshots/
       • Interprétations automatiques
    
    ✅ 5. HISTORISATION BATCH
       • Job batch périodique
       • Partitionnement par date
       • Rapports quotidiens
    
    ════════════════════════════════════════════════════════════════════════
    """)
    
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Pipeline complet Real-Time Stock Insight",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  python run_all.py                    # Pipeline complet (5 minutes)
  python run_all.py --duration 600     # Pipeline complet (10 minutes)
  python run_all.py --mode delta       # Delta Lake uniquement
  python run_all.py --mode ml          # ML uniquement
  python run_all.py --mode dashboard   # Dashboard uniquement
        """
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
    
    # Créer et lancer le pipeline
    app = CompletePipeline()
    
    try:
        if args.mode == "full":
            print(f"\n🚀 Démarrage du pipeline complet (durée: {args.duration}s)...\n")
            app.run_complete_pipeline(duration=args.duration)
        elif args.mode == "delta":
            print(f"\n📦 Démarrage du pipeline Delta Lake uniquement...\n")
            app.start_data_generation(duration=args.duration)
            app.start_delta_pipeline()
            print("\n✅ Pipeline Delta Lake démarré. Appuyez sur Ctrl+C pour arrêter.")
            import time
            time.sleep(args.duration)
        elif args.mode == "ml":
            print(f"\n🤖 Démarrage du pipeline ML uniquement...\n")
            app.start_data_generation(duration=args.duration)
            app.start_delta_pipeline()
            app.run_ml_training_and_scoring()
            print("\n✅ Pipeline ML démarré. Appuyez sur Ctrl+C pour arrêter.")
            import time
            time.sleep(args.duration)
        elif args.mode == "dashboard":
            print(f"\n📊 Génération du dashboard uniquement...\n")
            app.generate_dashboard()
        elif args.mode == "batch":
            print(f"\n📦 Exécution du job batch d'historisation...\n")
            app.run_batch_historization()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Interruption par l'utilisateur")
    except Exception as e:
        print(f"\n\n❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if args.mode == "full":
            app.cleanup()

if __name__ == "__main__":
    main()

