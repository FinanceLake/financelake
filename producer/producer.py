# producer.py
from kafka import KafkaProducer
import json
import time
from datetime import datetime
import yfinance as yf
import random

class YahooFinanceProducer:
    def __init__(self, bootstrap_servers='kafka:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            request_timeout_ms=30000
        )
        self.symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NFLX", "NVDA"]
        print("Yahoo Finance Producer initialized")
        
    def get_real_stock_data(self, symbol):
        """Fetch real data from Yahoo Finance"""
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            
            # Current price
            current_price = info.get('currentPrice') or info.get('regularMarketPrice') or info.get('previousClose')
            
            if current_price is None:
                print(f"No price for {symbol}, Using fallback")
                return self.get_fallback_data(symbol)

            # Recent history
            history = stock.history(period="2d", interval="1m")
            
            if not history.empty and len(history) > 0:
                latest_data = history.iloc[-1]
                previous_data = history.iloc[0]
                
                volume = int(latest_data.get('Volume', random.randint(100000, 1000000)))
                open_price = float(previous_data.get('Open', current_price * 0.98))
                high_price = float(latest_data.get('High', current_price * 1.02))
                low_price = float(latest_data.get('Low', current_price * 0.98))
                
                # Calculate the change
                price_variation = round(((current_price - open_price) / open_price) * 100, 2) if open_price > 0 else 0.0
                
            else:
                # No history available, use estimates
                volume = random.randint(100000, 1000000)
                open_price = float(current_price) * random.uniform(0.98, 1.0)
                high_price = float(current_price) * random.uniform(1.0, 1.02)
                low_price = float(current_price) * random.uniform(0.98, 1.0)
                price_variation = round(random.uniform(-3, 3), 2)
            
            # FULL STRUCTURE
            data = {
                "symbol": symbol,
                "price": round(float(current_price), 2),
                "volume": int(volume),
                "open": round(float(open_price), 2),
                "high": round(float(high_price), 2),
                "low": round(float(low_price), 2),
                "timestamp": datetime.now().isoformat(),
                "price_variation": float(price_variation),
                "price_increase": 1 if price_variation > 0 else 0
            }
            
            # CHECK: all fields are present
            required_fields = ["symbol", "price", "volume", "open", "high", "low", "timestamp", "price_variation", "price_increase"]
            missing_fields = [f for f in required_fields if f not in data]
            if missing_fields:
                print(f"ERROR: Missing fields for {symbol}: {missing_fields}")
                return self.get_fallback_data(symbol)
            
            return data
            
        except Exception as e:
            print(f"Yahoo Finance error for {symbol}: {e}")
            return self.get_fallback_data(symbol)
    
    def get_fallback_data(self, symbol):
        """Full simulated data if Yahoo Finance fails"""
        base_prices = {
            "AAPL": 180, "GOOGL": 140, "MSFT": 330, "AMZN": 150, 
            "TSLA": 240, "META": 320, "NFLX": 500, "NVDA": 450
        }
        
        base_price = base_prices.get(symbol, 200)
        variation = random.uniform(-0.03, 0.03)
        current_price = round(base_price * (1 + variation), 2)
        open_price = round(base_price, 2)
        
        data = {
            "symbol": symbol,
            "price": float(current_price),
            "volume": int(random.randint(100000, 1000000)),
            "open": float(open_price),
            "high": round(float(current_price * 1.02), 2),
            "low": round(float(current_price * 0.98), 2),
            "timestamp": datetime.now().isoformat(),
            "price_variation": round(float(variation * 100), 2),
            "price_increase": 1 if variation > 0 else 0
        }
        return data
    
    def start_producing(self):
        """Start producing data from Yahoo Finance"""
        print("Starting Yahoo Finance producer...")
        print(f"Tracked symbols: {', '.join(self.symbols)}")
        
        message_count = 0
        
        while True:
            try:
                for symbol in self.symbols:
                    data = self.get_real_stock_data(symbol)
                    
                    if data:
                        # Send to Kafka
                        self.producer.send("stock_prices", value=data)
                        message_count += 1
                        
                        trend = "📈 " if data['price_increase'] == 1 else "📉"
                        
                        # Display
                        print(f"{trend} [{message_count}] {data['symbol']}: ${data['price']} "
                              f"(O: ${data['open']}, H: ${data['high']}, L: ${data['low']}) "
                              f"Δ: {data['price_variation']}%  Vol: {data['volume']:,}")
                    
                    time.sleep(2)  # Delay between symbols to avoid Yahoo rate limiting
                
                print(f" Full cycle ({message_count} Messages sent)\n")
                time.sleep(30)  # Pause between cycles
                
            except KeyboardInterrupt:
                print("\n Stopping producer...")
                self.producer.close()
                break
            except Exception as e:
                print(f"Production error: {e}")
                time.sleep(10)

if __name__ == "__main__":
    producer = YahooFinanceProducer()
    producer.start_producing()