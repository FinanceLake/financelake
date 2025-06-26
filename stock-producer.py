import yfinance as yf
import json
import logging
from kafka import KafkaProducer
from datetime import datetime, timedelta, time
from redis_cache.redis_cache import get_stock_data, invalidate_ticker_cache, start_metrics_server
from prometheus_client import start_http_server, Counter
import pytz
import time

# Set up logging messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("kafka").setLevel(logging.WARNING)

stock_requests_total = Counter('stock_requests_total', 'Total number of stock data requests')
stock_requests_success = Counter('stock_requests_success_total', 'Number of successful stock data requests')
stock_requests_failure = Counter('stock_requests_failure_total', 'Number of failed stock data requests')


def get_default_dates():
    now = datetime.now()
    yesterday = now - timedelta(days=1)
    default_str = yesterday.strftime("%Y-%m-%d")
    return default_str, default_str


def fetch_stock_data_from_yfinance(symbol, start_date=None, end_date=None):
    logging.info(f"Fetching data from yfinance API for {symbol}")
    ticker = yf.Ticker(symbol)

    if start_date and end_date:
        # ajouter 1 jour à end_date pour inclure la journée complète
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        end_date_plus_one = end_date_dt.strftime("%Y-%m-%d")
        data = ticker.history(start=start_date, end=end_date_plus_one)
    else:
        today = datetime.today()
        seven_days_ago = today - timedelta(days=7)
        data = ticker.history(start=seven_days_ago.strftime("%Y-%m-%d"),
                              end=today.strftime("%Y-%m-%d"))

    if data.empty:
        logging.warning(f"No data returned for {symbol} from {start_date} to {end_date}")
        return None

    latest = data.iloc[-1]
    result = {
        "ticker": symbol,
        "start_date": start_date or latest.name.strftime("%Y-%m-%d"),
        "end_date": end_date or latest.name.strftime("%Y-%m-%d"),
        "price_data": {
            "date": latest.name.strftime("%Y-%m-%d"),
            "open": float(latest["Open"]),
            "high": float(latest["High"]),
            "low": float(latest["Low"]),
            "close": float(latest["Close"]),
            "volume": int(latest["Volume"])
        },
        "timestamp": datetime.now().isoformat()
    }
    logging.info(f"Fetched data from API: {result}")
    return result


def fetch_stock_data(symbol="AAPL", start_date=None, end_date=None, force_refresh=False):
    """
    Fetch stock data with caching support.
    
    Args:
        symbol: Stock ticker symbol
        start_date: Start date (YYYY-MM-DD format)
        end_date: End date (YYYY-MM-DD format)  
        force_refresh: If True, bypass cache and fetch fresh data
    
    Returns:
        dict: Stock data or None if fetch failed
    """
    if not start_date or not end_date:
        start_date, end_date = get_default_dates()

    stock_requests_total.inc()  # <-- INCRÉMENTATION

    try:
        data = get_stock_data(
            ticker=symbol,
            start_date=start_date,
            end_date=end_date,
            fetch_from_api_fn=fetch_stock_data_from_yfinance,
            force_refresh=force_refresh
        )
        if data:
            stock_requests_success.inc()  # <-- INCRÉMENTATION
        else:
            stock_requests_failure.inc()  # <-- INCRÉMENTATION
        return data
    except Exception as e:
        stock_requests_failure.inc()
        logging.error(f"Failed to fetch stock data for {symbol}: {e}")
        return None

def invalidate_stock_cache(symbol):
    """Invalidate all cached data for a specific stock ticker."""
    try:
        invalidate_ticker_cache(symbol)
        logging.info(f"Cache invalidated for ticker: {symbol}")
    except Exception as e:
        logging.error(f"Failed to invalidate cache for {symbol}: {e}")

def main():
    topic = "stock-data"
    symbol = "AAPL"  # Changed from "Gold" to proper ticker
    
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092", 
        value_serializer=lambda data: json.dumps(data).encode('utf-8')
    )
    
    # Fetch data using cached approach
    data = fetch_stock_data(symbol)
    
    # Send fetched data to Kafka
    if data:
        try:
            producer.send(topic, value=data)
            producer.flush()
            logging.info(f"Successfully sent data to Kafka topic '{topic}'")
        except Exception as e:
            logging.error(f"Failed to send data to Kafka: {e}")
    else:
        logging.error("No data to send to Kafka")

if __name__ == "__main__":
    start_metrics_server(port=8000)
    main()
    
    # Keep the process alive so that Prometheus metrics server keeps running
    try:
        while True:
            time.sleep(30)  # sleep and avoid busy wait
    except KeyboardInterrupt:
        print("Shutting down stock-producer with metrics...")
    
