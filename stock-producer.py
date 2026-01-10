import yfinance as yf
import json
import logging
from kafka import KafkaProducer
from datetime import datetime

# Set up logging messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("kafka").setLevel(logging.WARNING)

#fetch data for a symbol 
def fetch_stock_data(symbol="Gold"):
    logging.info(f"Fetching data for {symbol}")
    ticker = yf.Ticker(symbol)
    todays_data = ticker.history(period='1d')
    
    if todays_data.empty:
        logging.warning("No data returned.")
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
    topic = "stock-data"
    #Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092", 
        value_serializer=lambda data: json.dumps(data).encode('utf-8')
    )

    data = fetch_stock_data()
    # send fetched data to Kafka
    if data:
        try:
            producer.send(topic, value=data)
            producer.flush()
            logging.info(f"Successfully sent data to Kafka topic '{topic}'")
        except Exception as e:
            logging.error(f"Failed to send data to Kafka: {e}")

if __name__ == "__main__":
    main()
