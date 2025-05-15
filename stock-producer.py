import yfinance as yf
import json
import logging
from kafka import KafkaProducer
from datetime import datetime
import time
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "WARNING").upper()),
                    format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("kafka").setLevel(logging.WARNING)
#fech data 
def fetch_stock_data(symbol):
    logging.info(f"Fetching data for {symbol}") 
    ticker = yf.Ticker(symbol)
    todays_data = ticker.history(period='1d')
    
    if todays_data.empty:
        logging.warning(f"No data returned for {symbol}")
        return None

    latest = todays_data.iloc[-1]
    data = {
        "symbol": symbol,
        "date": latest.name.strftime("%Y-%m-%d"),
        "open": float(latest["Open"]),
        "high": float(latest["High"]),
        "low": float(latest["Low"]),
        "close": float(latest["Close"]),
        "volume": int(latest["Volume"])
    }

    logging.info(f"Fetched data: {data}")
    return data

def main():
    # Load variables
    kafka_broker = os.getenv("KAFKA_BROKERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "stock-data")
    symbols = os.getenv("SYMBOLS_LIST", "GOLD").split(",")
    interval = int(os.getenv("DATA_FETCH_INTERVAL", 300))

    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda data: json.dumps(data).encode('utf-8')
    )

    while True:
        for symbol in symbols:
            data = fetch_stock_data(symbol.strip())
            if data:
                try:
                    producer.send(topic, value=data)
                    producer.flush()
                    logging.info(f"Sent data for {symbol} to Kafka topic '{topic}'")
                except Exception as e:
                    logging.error(f"Failed to send data for {symbol} to Kafka: {e}")
        logging.info(f"Waiting {interval} seconds before next fetch")
        time.sleep(interval)

if __name__ == "__main__":
    main()
