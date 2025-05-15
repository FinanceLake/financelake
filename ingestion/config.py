from dotenv import load_dotenv
import os

load_dotenv()

TOPIC = os.getenv("TOPIC")
HDFS_BASE_DIR = os.getenv("HDFS_BASE_DIR")
LOCAL_LOG_DIR = os.getenv("LOCAL_LOG_DIR")
API_KEY = os.getenv("API_KEY")
SYMBOL = os.getenv("SYMBOL")
TOPIC_NAME = os.getenv("TOPIC_NAME")

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
HDFS_URL= 'hdfs://localhost:9000'
