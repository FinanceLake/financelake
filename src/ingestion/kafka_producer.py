import json
from dotenv import load_dotenv
from kafka import KafkaProducer
import logging
import os


load_dotenv()

logging.basicConfig(
    filename='logs/kafka_producer.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    filemode='w'
)

class FinanceLakeKafkaProducer:
   
    def __init__(self):
        self.broker = os.getenv("KAFKA_BROKER", "redpanda:19094")
        self.topic = os.getenv("TOPIC_NAME", "stock_prices")

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[self.broker],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=3,
                request_timeout_ms=30000,
                api_version=(0, 10, 1)
            )
            logging.info(f"[Kafka] Connected to {self.broker}, topic={self.topic}")
        except Exception as e:
            logging.error(f"[ERROR] Could not connect to Kafka: {e}")
            self.producer = None

    def send(self, data: dict):
        if not self.producer:
            logging.error("[ERROR] Producer not initialized.")
            return

        try:
            self.producer.send(self.topic, value=data)
            logging.info(f"[KAFKA PUSH] record = {data}")
        except Exception as e:
            logging.error(f"[ERROR] Failed to send message: {e}")

    def close(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logging.info("[Kafka] Connection closed.")
