# Visualisation Simple - Temps Réel
import json
import time
import os
from pathlib import Path
from datetime import datetime

DATA_DIR = Path(__file__).parent / "data" / "gold"
PREDICTIONS_FILE = DATA_DIR / "redis_predictions.json"

def clear_screen():
    """Efface l'écran"""
    os.system('cls' if os.name == 'nt' else 'clear')

def load_predictions():
    """Charge les prédictions"""
    if PREDICTIONS_FILE.exists():
        try:
            with open(PREDICTIONS_FILE, 'r') as f:
                return json.load(f)
        except:
            return []
    return []

def display_dashboard(predictions):
    """Affiche le dashboard"""
    clear_screen()
    
    num = len(predictions)
    
    print("=" * 80)
    print("📊 AMZN - PRÉDICTIONS EN TEMPS RÉEL (Redis + LSTM)".center(80))
    print("=" * 80)
    print()
    
    # Métriques
    print(f"  📈 Total Prédictions: {num}".ljust(40), end="")
    print(f"  ⏱️  Mise à jour: {time.strftime('%H:%M:%S')}")
    print()
    
    if predictions:
        last = predictions[-1]
        
        print(f"  💰 Dernier Prix:      ${last['current_price']:.2f}".ljust(40), end="")
        print(f"  🎯 Prédiction:        ${last['predicted_price']:.2f}")
        
        change = last['percent_change']
        emoji = "📈" if change > 0 else "📉"
        print(f"  {emoji} Erreur:            {change:+.2f}%".ljust(40), end="")
        print(f"  💹 Changement:        ${last['change']:+.2f}")
    
    print()
    print("=" * 80)
    
    # Tableau des dernières 10 prédictions
    print("\n📋 DERNIÈRES 10 PRÉDICTIONS:\n")
    print(f"{'#':>4} | {'Prix Actuel':>12} | {'Prédiction':>12} | {'Erreur':>10} | {'Timestamp':>19}")
    print("-" * 80)
    
    for i, pred in enumerate(predictions[-10:]):
        idx = num - 10 + i
        emoji = "📈" if pred['percent_change'] > 0 else "📉"
        print(f"{idx:>4} | ${pred['current_price']:>11.2f} | ${pred['predicted_price']:>11.2f} | {emoji} {pred['percent_change']:>8.2f}% | {pred['timestamp']}")
    
    print()
    print("=" * 80)
    print("En attente des nouvelles données... (Appuyez sur Ctrl+C pour arrêter)")
    print("=" * 80)

def main():
    """Boucle principale"""
    print("🚀 Démarrage du monitoring en temps réel...")
    time.sleep(2)
    
    last_count = 0
    
    while True:
        try:
            predictions = load_predictions()
            current_count = len(predictions)
            
            # Afficher seulement si nouvelles données
            if current_count != last_count:
                display_dashboard(predictions)
                last_count = current_count
            
            time.sleep(1)  # Vérifier chaque seconde
        
        except KeyboardInterrupt:
            print("\n\n✅ Monitoring arrêté")
            break
        except Exception as e:
            print(f"❌ Erreur: {e}")
            time.sleep(2)

if __name__ == "__main__":
    main()
