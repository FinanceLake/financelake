# Point d'entr�e principal
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import numpy as np
import pandas as pd
import json
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

from config import Config
from src.pipeline import DataPipeline
from src.model import LSTMModel
from src.visualization import Visualization

def main():
    """Pipeline principal"""
    print("=" * 70)
    print("PROJET AMZN - PRÉDICTION DE PRIX AVEC MODÈLE LSTM")
    print("=" * 70)
    
    # Initialisation
    Config.create_directories()
    pipeline = DataPipeline()
    visualizer = Visualization()
    
    # 1. Chargement des données
    print("\n[1/5] CHARGEMENT DES DONNÉES")
    print("-" * 40)
    df = pipeline.load_data()
    print(f"✓ Données chargées: {len(df)} lignes")
    print(f"  Période: {df['Date'].min().strftime('%Y-%m-%d')} au {df['Date'].max().strftime('%Y-%m-%d')}")
    print(f"  Prix moyen: ${df['Close'].mean():.2f}")
    
    # 2. Création des features
    print("\n[2/5] CRÉATION DES FEATURES")
    print("-" * 40)
    df_features = pipeline.create_features(df)
    print(f"✓ Features créées: {len(df_features.columns)} colonnes")
    print(f"  Données après nettoyage: {len(df_features)} lignes")
    
    # 3. Préparation des données LSTM
    print("\n[3/5] PRÉPARATION DES DONNÉES LSTM")
    print("-" * 40)
    X_train, X_test, y_train, y_test = pipeline.prepare_lstm_data(df_features)
    
    # Sauvegarde des scalers
    import pickle
    scalers = {
        'scaler_X': pipeline.scaler_X,
        'scaler_y': pipeline.scaler_y
    }
    with open(Config.GOLD_DIR / 'scalers.pkl', 'wb') as f:
        pickle.dump(scalers, f)
    
    # 4. Entraînement du modèle
    print("\n[4/5] ENTRAÎNEMENT DU MODÈLE LSTM")
    print("-" * 40)
    model = LSTMModel()
    history = model.train(X_train, y_train, X_test, y_test)
    
    # Évaluation
    metrics, predictions, actual = model.evaluate(X_test, y_test, pipeline.scaler_y)
    
    # 5. Prédictions futures
    print("\n[5/5] GÉNÉRATION DES PRÉDICTIONS")
    print("-" * 40)
    
    # Dernière séquence pour la prédiction
    last_sequence = X_test[-1]
    future_predictions = model.predict_future(
        last_sequence, 
        pipeline.scaler_X, 
        pipeline.scaler_y, 
        days=3
    )
    
    # Sauvegarde des prédictions
    predictions_data = {
        'symbol': 'AMZN',
        'generation_date': datetime.now().isoformat(),
        'last_training_date': df_features['Date'].max().strftime('%Y-%m-%d'),
        'last_actual_price': float(df_features['Close'].iloc[-1]),
        'model_metrics': metrics,
        'predictions': future_predictions
    }
    
    predictions_path = Config.GOLD_DIR / Config.PREDICTIONS_FILE
    with open(predictions_path, 'w') as f:
        json.dump(predictions_data, f, indent=2)
    
    print(f"✓ Prédictions sauvegardées: {predictions_path}")
    print("\n📊 PRÉDICTIONS POUR LES 3 PROCHAINS JOURS:")
    print("-" * 40)
    for pred in future_predictions:
        print(f"  Jour {pred['day']} ({pred['date']}): ${pred['predicted_price']:.2f}")
    
    # 6. Visualisation
    print("\n[6/6] CRÉATION DE LA VISUALISATION")
    print("-" * 40)
    
    fig = visualizer.create_main_visualization(df, future_predictions, metrics)
    html_path = visualizer.save_visualization(fig)
    
    print("\n" + "=" * 70)
    print("✅ PIPELINE TERMINÉ AVEC SUCCÈS!")
    print("=" * 70)
    print(f"\n📁 FICHIERS GÉNÉRÉS:")
    print(f"  • Données brutes: {Config.BRONZE_DIR / Config.CSV_FILE}")
    print(f"  • Données avec features: {Config.SILVER_DIR / 'amzn_features.csv'}")
    print(f"  • Modèle LSTM: {Config.GOLD_DIR / Config.MODEL_FILE}")
    print(f"  • Prédictions: {predictions_path}")
    print(f"  • Visualisation: {html_path}")
    
    print(f"\n📈 RÉSUMÉ DES PERFORMANCES:")
    print(f"  • RMSE: ${metrics.get('RMSE', 0):.2f}")
    print(f"  • MAE: ${metrics.get('MAE', 0):.2f}")
    print(f"  • R²: {metrics.get('R2', 0):.4f}")
    
    print(f"\n🎯 PRÉDICTION FINALE:")
    last_pred = future_predictions[-1]
    print(f"  Prix AMZN dans 3 jours ({last_pred['date']}): ${last_pred['predicted_price']:.2f}")
    
    print(f"\n📊 Pour voir la visualisation interactive:")
    print(f"  Ouvrez le fichier: {html_path}")
    print("=" * 70)

if __name__ == "__main__":
    main()