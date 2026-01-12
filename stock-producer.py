import json
import logging
import random
import time
from kafka import KafkaProducer
from datetime import datetime

# Set up logging messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("kafka").setLevel(logging.WARNING)

# Mock data generator for demo purposes
def fetch_stock_data(symbol="AAPL"):
    """Generate mock stock data for demo"""
    logging.info(f"Generating mock data for {symbol}")

    # Base prices for different symbols
    base_prices = {
        "AAPL": 180.0,
        "GOOGL": 140.0,
        "MSFT": 380.0,
        "AMZN": 155.0,
        "TSLA": 220.0,
        "NVDA": 850.0,
        "META": 330.0,
        "NFLX": 480.0
    }

    base_price = base_prices.get(symbol, 100.0)

    # Add some random variation
    variation = random.uniform(-5, 5)
    current_price = base_price + variation

    # Generate OHLC data
    open_price = current_price + random.uniform(-2, 2)
    high_price = max(open_price, current_price) + random.uniform(0, 3)
    low_price = min(open_price, current_price) - random.uniform(0, 3)
    close_price = current_price + random.uniform(-1, 1)

    data = {
        "symbol": symbol,
        "timestamp": datetime.now().isoformat(),
        "open": round(open_price, 2),
        "high": round(high_price, 2),
        "low": round(low_price, 2),
        "close": round(close_price, 2),
        "volume": random.randint(100000, 10000000),
        "price_change": round(close_price - open_price, 2),
        "rsi": round(random.uniform(20, 80), 2),
        "macd": round(random.uniform(-2, 2), 2),
        "volatility": round(random.uniform(0.5, 5.0), 2)
    }

    logging.info(f"Fetched data: {data}")
    return data

def main():
    topic = "stock-data"
    symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "NFLX"]

    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda data: json.dumps(data).encode('utf-8')
    )

    logging.info("Stock Data Producer started - generating mock data every 2 seconds")

    message_count = 0

    while True:
        try:
            # Cycle through symbols
            for symbol in symbols:
                data = fetch_stock_data(symbol)

                if data:
                    # Send data to Kafka
                    producer.send(topic, value=data)
                    producer.flush()

                    message_count += 1
                    logging.info(f"[{message_count}] Sent {symbol} data to Kafka topic '{topic}'")

                    # Small delay between symbols
                    time.sleep(0.5)

            # Wait before next cycle
            logging.info(f"Completed cycle for {len(symbols)} symbols. Waiting 2 seconds...")
            time.sleep(2)

        except KeyboardInterrupt:
            logging.info("Producer stopped by user")
            break
        except Exception as e:
            logging.error(f"Error in producer loop: {e}")
            time.sleep(5)  # Wait before retrying

if __name__ == "__main__":
    main()
