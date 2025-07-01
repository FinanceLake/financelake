import json
import logging
from kafka import KafkaProducer

def create_kafka_producer(broker_url):
    try:
        producer = KafkaProducer(
            bootstrap_servers=broker_url,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            retries=5,
            linger_ms=10,
        )
        logging.info("Kafka Producer created successfully.")
        return producer
    except Exception as e:
        logging.error(f"Failed to create Kafka producer: {e}")
        return None

def send_to_kafka(producer, topic, data):
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