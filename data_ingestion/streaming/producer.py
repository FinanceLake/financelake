import json
import logging
import time
import requests
from kafka import KafkaProducer
from symbol_loader import get_sp500_symbols
import config
import os
from dotenv import load_dotenv

load_dotenv()


# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)



API_KEY = os.getenv('API_KEY_ALPHA_VANTAGE')
TOPIC_NAME = os.getenv('KAFKA_TOPIC')
SYMBOL_SLEEP = 12  # Alpha Vantage free tier: 5 requests/minute


def create_kafka_producer():
    """Creates and returns a Kafka producer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=os.getenv('KAFKA_BROKER_URL'),
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            retries=5,
            linger_ms=10,
        )
        logging.info("Kafka Producer created successfully.")
        return producer
    except Exception as e:
        logging.error(f"Failed to create Kafka producer: {e}")
        return None


def fetch_stock_data(symbol):
    """Fetches real-time stock data for a given symbol using Alpha Vantage API."""
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=1min&apikey={API_KEY}'
    try:
        response = requests.get(url)
        data = response.json()
        time_series = data.get('Time Series (1min)', {})
        if time_series:
            latest_timestamp = sorted(time_series.keys())[-1]
            latest_data = time_series[latest_timestamp]
            message = {
                'symbol': symbol,
                'timestamp': latest_timestamp,
                'open': float(latest_data['1. open']),
                'high': float(latest_data['2. high']),
                'low': float(latest_data['3. low']),
                'close': float(latest_data['4. close']),
                'volume': int(latest_data['5. volume'])
            }
            logging.info(f"Fetched data: {message}")
            return message
        else:
            note = data.get('Note', 'Unknown error')
            logging.warning(f"No data for {symbol}: {note}")
            if 'frequency' in note or 'limit' in note:
                logging.warning("Rate limit reached, pausing for 2 minutes...")
                time.sleep(120)
            return None
    except Exception as e:
        logging.error(f"Error fetching data for {symbol}: {e}")
        return None


def send_to_kafka(producer, topic, data):
    """Sends data to a Kafka topic."""
    if not data:
        return
    try:
        future = producer.send(topic, value=data)
        record_metadata = future.get(timeout=10)
        logging.info(
            f"Successfully sent data to topic '{topic}' at partition {record_metadata.partition}"
        )
        producer.flush()
    except Exception as e:
        logging.error(f"Failed to send data to Kafka: {e}")


def main():
    """Main function to run the streaming producer."""
    producer = create_kafka_producer()
    if not producer:
        return

    tickers = get_sp500_symbols()
    if not tickers:
        logging.error("No symbols loaded. Exiting script.")
        return

    while True:
        for symbol in tickers:
            stock_data = fetch_stock_data(symbol)
            if stock_data:
                send_to_kafka(producer, TOPIC_NAME, stock_data)
            time.sleep(SYMBOL_SLEEP)  # Respect Alpha Vantage rate limit
        logging.info(f"Waiting for {config.FETCH_INTERVAL_SECONDS} seconds before next round...")
        time.sleep(config.FETCH_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()