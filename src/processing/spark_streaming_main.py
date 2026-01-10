from processing.spark_streaming_processor import StockStreamingProcessor
from processing.spark_streaming_utils import setup_logging

logger = setup_logging()

def main():
    processor = StockStreamingProcessor()
    processor.start_streaming()

if __name__ == "__main__":
    main()