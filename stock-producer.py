import yfinance as yf
import json
import logging
from kafka import KafkaProducer
import os
import time

# Set up logging messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("kafka").setLevel(logging.WARNING)

def fetch_stock_data(symbols: list[str]):
    """
    Fetches the latest trading day's historical stock data for a *list* of given symbols using yfinance.

    Args:
        symbols (list[str]): A list of stock symbols (e.g., ["AAPL", "MSFT", "GOOG"]).

    Returns:
        list[dict]: A list of data dictionaries, where each dictionary contains
                    the latest trading day's data for a corresponding symbol. 
                    The 'symbol' key is included in each dictionary, making it identifiable.
                    Returns an empty list if no data is successfully fetched for any of the symbols.
    """
    all_data = []
    # Loop through each stock symbol provided in the 'symbols' list
    for symbol in symbols:
        logging.info(f"Fetching data for {symbol}")
        ticker = yf.Ticker(symbol)
        todays_data = ticker.history(period='1d')
        
        if todays_data.empty:
            logging.warning(f"No data returned for {symbol}.")
            continue

        latest = todays_data.iloc[-1]
        # Construct a data dictionary for the current symbol.
        data = {
            "symbol": symbol, # Stock symbol (e.g., "AAPL")
            "date": latest.name.strftime("%Y-%m-%d"), # Date of the fetched data
            "open": float(latest["Open"]), # Opening price for the day
            "high": float(latest["High"]), # Highest price reached during the day
            "low": float(latest["Low"]), # Lowest price reached during the day
            "close": float(latest["Close"]), # Closing price for the day
            "volume": int(latest["Volume"]) # Volume of shares traded
        }

        logging.info(f"Fetched data for {symbol}: {data}")
        all_data.append(data)

    return all_data

def main():
    topic = "stock-data" # Kafka topic to which messages will be produced.
    # target_symbols defines the list of financial instruments 
    target_symbols = ["AAPL", "MSFT", "GOOG", "GC=F"]  # Example: Apple, Microsoft, Google, Gold Futures

    
    #Create a Kafka producer
    KAFKA_SERVER = os.getenv("KAFKA_BROKER", "localhost:9092")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER, 
        value_serializer=lambda data: json.dumps(data).encode('utf-8')
    )

    # to continuously fetch and send data 
    while True:
        # Fetch data for all symbols specified in the target_symbols list.
        fetched_data_list = fetch_stock_data(target_symbols)
        
        if not fetched_data_list:
            logging.info("No data fetched for any symbols. Exiting.")
            return

        # Iterate through the list of fetched data. Each 'data_item' in this list
        # corresponds to the data for a single stock symbol.
        # Each data_item is then sent as a separate message to the Kafka topic.
        for data_item in fetched_data_list:
            if data_item: # Ensure data_item is not None
                try:
                    # Send the individual data dictionary (for a specific symbol) to the Kafka topic.
                    producer.send(topic, value=data_item)
                    producer.flush()
                    logging.info(f"Successfully sent data for {data_item['symbol']} to Kafka topic '{topic}'")
                except Exception as e:
                    logging.error(f"Failed to send data for {data_item['symbol']} to Kafka: {e}")
            time.sleep(10)  # Wait 10 seconds before fetching again

if __name__ == "__main__":
    main()
