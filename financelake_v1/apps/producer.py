import json
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
import yfinance as yf

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = "kafka:9092"
TOPIC = "stocks"
SYMBOLS = ["ETH-USD", "GOLD", "NVDA", "TSLA", "BTC-USD"]

def fetch_and_send():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks='all'
    )

    while True:
        for symbol in SYMBOLS:
            try:
                ticker = yf.Ticker(symbol)
                price = ticker.fast_info['last_price']
                data = {
                    "symbol": symbol,
                    "price": round(price, 2),
                    "timestamp": datetime.utcnow().isoformat()
                }
                producer.send(TOPIC, data)
                logger.info(f"✅ Sent {symbol}: {data['price']}")
            except Exception as e:
                logger.error(f"❌ Error {symbol}: {e}")
        producer.flush()
        time.sleep(10)

if __name__ == "__main__":
    fetch_and_send()