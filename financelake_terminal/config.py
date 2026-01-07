from dotenv import load_dotenv
import os

# Load environment variables from project .env
load_dotenv()

# Base paths
BASE_PATH = os.getenv("BASE_PATH", "/tmp/financelake")
BASE_PATH_FILE = os.getenv("BASE_PATH_FILE", f"file://{BASE_PATH}")
# UI assets
LOGO_PATH = os.getenv("LOGO_PATH", "resources/img/logo.png")

BRONZE_PATH = os.getenv("BRONZE_PATH", f"{BASE_PATH_FILE}/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", f"{BASE_PATH_FILE}/silver")
GOLD_FEATURES_PATH = os.getenv("GOLD_FEATURES_PATH", f"{BASE_PATH_FILE}/gold/features")
GOLD_PREDICTIONS_PATH = os.getenv("GOLD_PREDICTIONS_PATH", f"{BASE_PATH_FILE}/gold/predictions")

BRONZE_CHECKPOINT = os.getenv("BRONZE_CHECKPOINT", f"{BASE_PATH_FILE}/bronze_checkpoint")
SILVER_CHECKPOINT = os.getenv("SILVER_CHECKPOINT", f"{BASE_PATH_FILE}/silver_checkpoint")
GOLD_PROCESSING_CHECKPOINT = os.getenv("GOLD_PROCESSING_CHECKPOINT", f"{BASE_PATH_FILE}/gold_processing_checkpoint")

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", os.getenv("KAFKA_BROKER", "localhost:9092"))
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "store_prices")

# Model
# Keep file:// format by default to match existing usage in notebooks
MODEL_PATH = os.getenv("MODEL_PATH", "file:///home/abdessamad/models/random_forest_classifier")
MODEL_OUTPUT_PATH = os.getenv("MODEL_OUTPUT_PATH", MODEL_PATH)

# Finnhub
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
