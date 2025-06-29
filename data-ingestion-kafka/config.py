from dotenv import load_dotenv
import os

load_dotenv()  # Load variables from .env file

# Kafka
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "finance-transactions")
KAFKA_SERVER = os.getenv("KAFKA_BROKER", "localhost:9092")


CSV_FILE = "data/creditcard.csv"

# HDFS
HDFS_USER = os.getenv("HDFS_USER", "hadoop")
HDFS_LOG_PATH = os.getenv("HDFS_LOG_PATH", "/user/hadoop/finance-data")
