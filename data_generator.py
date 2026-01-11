import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
import os
import socket
import traceback

class StockDataGenerator:
    def __init__(self):
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        if isinstance(bootstrap, str):
            bootstrap_servers = [x.strip() for x in bootstrap.split(",")]
        else:
            bootstrap_servers = bootstrap

        # Parse host/port for health check
        host, port = bootstrap_servers[0].split(":")
        port = int(port)

        print(f"Waiting for Kafka at {host}:{port} ...")

        # ---- WAIT UNTIL KAFKA IS READY ----
        while True:
            try:
                sock = socket.create_connection((host, port), timeout=3)
                sock.close()
                print("Kafka is UP! Starting producer...")
                break
            except OSError:
                print("Kafka not ready, retrying...")
                time.sleep(2)

        # ---- CREATE PRODUCER ----
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        self.topic = "stock_fin"
        self.stocks = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
        self.current_prices = {
            "AAPL": 150, "GOOGL": 2800, "MSFT": 330,
            "AMZN": 3400, "TSLA": 200
        }

    def generate_stock_data(self):
        symbol = random.choice(self.stocks)
        current_price = self.current_prices[symbol]

        price_change = random.uniform(-3, 3)
        new_price = max(current_price + price_change, current_price * 0.9)
        self.current_prices[symbol] = new_price

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": symbol,
            "price": round(new_price, 2),
            "volume": random.randint(1000, 50000),
            "rsi": round(random.uniform(20, 80), 2),
            "macd": round(random.uniform(-2, 2), 2),
            "volatility": round(random.uniform(0.5, 5.0), 2),
            "price_change": round(price_change, 2),
            "price_direction": 1 if price_change > 0 else 0,
            "volume_change": random.randint(-1000, 1000)
        }

    def start_streaming(self, interval=1):
        print(f"Streaming data to Kafka topic: {self.topic}")

        count = 0

        try:
            while True:
                data = self.generate_stock_data()
                self.producer.send(self.topic, data)
                count += 1

                if count % 10 == 0:
                    print(f"Sent {count}: {data['symbol']} - ${data['price']}")

                time.sleep(interval)

        except Exception as e:
            print("Error in generator:", e)
            traceback.print_exc()
        finally:
            self.producer.flush()
            self.producer.close()


if __name__ == "__main__":
    generator = StockDataGenerator()
    generator.start_streaming(interval=1)
