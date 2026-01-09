"""
Configuration file for the Spark Real-Time Stock Insight project
"""

# Stock symbols to simulate
STOCK_SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA"]

# Streaming configuration
WINDOW_DURATION = "10 seconds"  # Window size for aggregations
SLIDE_DURATION = "5 seconds"    # Slide interval for windows
CHECKPOINT_DIR = "./checkpoints"
OUTPUT_DIR = "./output"

# Data generation configuration
DATA_GENERATION_INTERVAL = 1  # seconds
KAFKA_TOPIC = "stock_prices"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Price simulation parameters
BASE_PRICES = {
    "AAPL": 180.0,
    "GOOGL": 140.0,
    "MSFT": 370.0,
    "AMZN": 145.0,
    "TSLA": 250.0,
    "META": 320.0,
    "NVDA": 480.0
}

# MLlib configuration
TRAIN_TEST_SPLIT_RATIO = 0.8
MODEL_CHECKPOINT_DIR = "./models"

# Delta Lake configuration
DELTA_BRONZE_PATH = "./delta/bronze"
DELTA_SILVER_PATH = "./delta/silver"
DELTA_GOLD_PATH = "./delta/gold"
DELTA_CHECKPOINT_DIR = "./checkpoints/delta"

# Dashboard configuration
DASHBOARD_OUTPUT_DIR = "./dashboard/screenshots"

