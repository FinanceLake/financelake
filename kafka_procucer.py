"""
Producers Kafka pour FinanceLake
Génère des données financières réalistes en streaming
"""

import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import requests

# ============================================
# CONFIGURATION
# ============================================
KAFKA_BROKER = "localhost:9092"
ALPHA_VANTAGE_KEY = "LAIIN8863N5LIZVV" 

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ============================================
# PRODUCER 1 : Stock Trades (données réelles)
# ============================================
def produce_stock_trades():
    """
    Récupère les prix Apple/Tesla/Google et simule des trades
    """
    symbols = ["AAPL", "TSLA", "GOOGL", "MSFT", "AMZN"]
    
    print("🔵 [STOCKS] Démarrage du producer...")
    
    while True:
        try:
            for symbol in symbols:
                # Prix réel depuis Alpha Vantage (limite 5 req/min en gratuit)
                url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={ALPHA_VANTAGE_KEY}"
                response = requests.get(url, timeout=5)
                
                if response.status_code == 200:
                    data = response.json()
                    if "Global Quote" in data:
                        price = float(data["Global Quote"]["05. price"])
                    else:
                        price = random.uniform(100, 500)  # Fallback si quota dépassé
                else:
                    price = random.uniform(100, 500)
                
                # Simuler un trade
                trade = {
                    "trade_id": f"TRD-{int(time.time())}-{random.randint(1000,9999)}",
                    "timestamp": datetime.now().isoformat(),
                    "symbol": symbol,
                    "price": round(price, 2),
                    "quantity": random.randint(1, 100),
                    "side": random.choice(["BUY", "SELL"]),
                    "client_id": f"CLT-{random.randint(1000,5000)}",
                    "exchange": "NASDAQ"
                }
                
                producer.send("stock-trades", trade)
                print(f"✓ Trade envoyé : {symbol} @ ${price:.2f}")
                
                time.sleep(2)  # 2 sec entre chaque symbole
            
            time.sleep(10)  # 10 sec avant le prochain cycle
            
        except Exception as e:
            print(f"❌ Erreur stock trades: {e}")
            time.sleep(5)

# ============================================
# PRODUCER 2 : Crypto Prices (CoinGecko)
# ============================================
def produce_crypto_prices():
    """
    Récupère BTC, ETH, BNB en temps réel
    """
    cryptos = ["bitcoin", "ethereum", "binancecoin", "cardano", "solana"]
    
    print("🟡 [CRYPTO] Démarrage du producer...")
    
    while True:
        try:
            url = f"https://api.coingecko.com/api/v3/simple/price?ids={','.join(cryptos)}&vs_currencies=usd&include_24hr_change=true"
            response = requests.get(url, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                
                for crypto in cryptos:
                    if crypto in data:
                        price_data = {
                            "crypto_id": crypto.upper(),
                            "timestamp": datetime.now().isoformat(),
                            "price_usd": data[crypto]["usd"],
                            "change_24h": data[crypto].get("usd_24h_change", 0),
                            "volume": random.randint(1000000, 10000000)  # Simulé
                        }
                        
                        producer.send("crypto-prices", price_data)
                        print(f"✓ Crypto envoyé : {crypto.upper()} @ ${data[crypto]['usd']:.2f}")
            
            time.sleep(30)  # Refresh toutes les 30 sec
            
        except Exception as e:
            print(f"❌ Erreur crypto prices: {e}")
            time.sleep(10)

# ============================================
# PRODUCER 3 : Bank Transactions (simulées)
# ============================================
def produce_bank_transactions():
    """
    Génère des transactions bancaires réalistes
    """
    transaction_types = [
        ("PAYMENT", 0.85, (10, 500)),      # 85% paiements normaux
        ("TRANSFER", 0.10, (100, 5000)),   # 10% virements
        ("WITHDRAWAL", 0.04, (50, 1000)),  # 4% retraits
        ("FRAUD", 0.01, (1000, 10000))     # 1% fraudes
    ]
    
    merchants = ["Amazon", "Carrefour", "Shell", "Netflix", "Spotify", 
                 "Apple Store", "Restaurant", "Pharmacy", "Hotel"]
    
    print("🟢 [BANK] Démarrage du producer...")
    
    while True:
        try:
            # Choisir le type de transaction selon les probabilités
            rand = random.random()
            cumul = 0
            for tx_type, prob, amount_range in transaction_types:
                cumul += prob
                if rand <= cumul:
                    selected_type = tx_type
                    amount = round(random.uniform(*amount_range), 2)
                    break
            
            transaction = {
                "transaction_id": f"TXN-{int(time.time())}-{random.randint(10000,99999)}",
                "timestamp": datetime.now().isoformat(),
                "client_id": f"CLT-{random.randint(1000,5000)}",
                "amount": amount,
                "currency": "EUR",
                "type": selected_type,
                "merchant": random.choice(merchants) if selected_type == "PAYMENT" else None,
                "card_last4": f"*{random.randint(1000,9999)}",
                "is_fraud": 1 if selected_type == "FRAUD" else 0
            }
            
            producer.send("bank-transactions", transaction)
            
            status = "🚨 FRAUD" if selected_type == "FRAUD" else "✓"
            print(f"{status} Transaction : {selected_type} {amount}€")
            
            # Fréquence : 1 transaction toutes les 2-5 secondes
            time.sleep(random.uniform(2, 5))
            
        except Exception as e:
            print(f"❌ Erreur bank transactions: {e}")
            time.sleep(3)

# ============================================
# MAIN : Lancer tous les producers
# ============================================
if __name__ == "__main__":
    import threading
    
    print("=" * 60)
    print("🚀 DÉMARRAGE DES PRODUCERS KAFKA")
    print("=" * 60)
    
    # Lancer les 3 producers en parallèle
    threads = [
        threading.Thread(target=produce_stock_trades, daemon=True),
        threading.Thread(target=produce_crypto_prices, daemon=True),
        threading.Thread(target=produce_bank_transactions, daemon=True)
    ]
    
    for t in threads:
        t.start()
    
    # Garder le programme actif
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n🛑 Arrêt des producers...")
        producer.close()
