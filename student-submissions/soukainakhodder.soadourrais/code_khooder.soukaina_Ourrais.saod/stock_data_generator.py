"""
Stock Data Generator - Simule des données boursières en temps réel
Ce script génère des données de prix d'actions et les écrit dans des fichiers JSON
pour être consommées par Spark Structured Streaming
"""

import json
import random
import time
from datetime import datetime
import os
from config import STOCK_SYMBOLS, BASE_PRICES, DATA_GENERATION_INTERVAL, OUTPUT_DIR

class StockDataGenerator:
    """
    Générateur de données boursières simulées
    """
    
    def __init__(self, output_path="./stream_data"):
        self.output_path = output_path
        self.prices = BASE_PRICES.copy()
        os.makedirs(output_path, exist_ok=True)
        
    def generate_stock_data(self, symbol):
        """
        Génère un enregistrement de données boursières pour un symbole donné
        
        La simulation inclut:
        - Prix avec variation aléatoire (-2% à +2%)
        - Volume de transactions aléatoire
        - Horodatage précis
        """
        # Variation du prix: -2% à +2%
        price_change_percent = random.uniform(-0.02, 0.02)
        new_price = self.prices[symbol] * (1 + price_change_percent)
        self.prices[symbol] = new_price
        
        # Volume aléatoire entre 100,000 et 1,000,000
        volume = random.randint(100000, 1000000)
        
        # Génération de l'enregistrement
        record = {
            "symbol": symbol,
            "price": round(new_price, 2),
            "volume": volume,
            "timestamp": datetime.now().isoformat(),
            "price_change": round(price_change_percent * 100, 4)
        }
        
        return record
    
    def generate_batch(self):
        """
        Génère un batch de données pour tous les symboles
        """
        batch = []
        for symbol in STOCK_SYMBOLS:
            record = self.generate_stock_data(symbol)
            batch.append(record)
        return batch
    
    def run(self, duration_seconds=300):
        """
        Exécute le générateur pendant une durée spécifiée
        
        Args:
            duration_seconds: Durée d'exécution en secondes (défaut: 5 minutes)
        """
        print(f"🚀 Démarrage du générateur de données boursières...")
        print(f"📊 Symboles: {', '.join(STOCK_SYMBOLS)}")
        print(f"📁 Dossier de sortie: {self.output_path}")
        print(f"⏱️  Intervalle: {DATA_GENERATION_INTERVAL} seconde(s)\n")
        
        start_time = time.time()
        batch_count = 0
        
        try:
            while (time.time() - start_time) < duration_seconds:
                batch = self.generate_batch()
                
                # Écriture dans un fichier JSON unique par batch
                filename = f"stock_batch_{int(time.time() * 1000)}.json"
                filepath = os.path.join(self.output_path, filename)
                
                with open(filepath, 'w') as f:
                    for record in batch:
                        f.write(json.dumps(record) + '\n')
                
                batch_count += 1
                print(f"✅ Batch #{batch_count} généré ({len(batch)} enregistrements) - {filename}")
                
                time.sleep(DATA_GENERATION_INTERVAL)
                
        except KeyboardInterrupt:
            print("\n⚠️  Interruption par l'utilisateur")
        
        elapsed_time = time.time() - start_time
        print(f"\n📈 Génération terminée!")
        print(f"   - Durée: {elapsed_time:.2f} secondes")
        print(f"   - Batches générés: {batch_count}")
        print(f"   - Total d'enregistrements: {batch_count * len(STOCK_SYMBOLS)}")

if __name__ == "__main__":
    generator = StockDataGenerator()
    # Générer des données pendant 5 minutes par défaut
    generator.run(duration_seconds=300)

