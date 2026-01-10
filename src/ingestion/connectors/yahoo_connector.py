import logging
import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
from ingestion.kafka_producer import FinanceLakeKafkaProducer
import yfinance as yf
from datetime import datetime, timezone
import time

# Configure logging
logging.basicConfig(
    filename='logs/yahoo_finance_connector.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    filemode='w'
)

class YahooFinanceConnector:
    def __init__(self, symbols=None, interval=5):
        self.symbols = symbols or ["AAPL", "GOOG", "AMZN", "MSFT", "TSLA", "META", "NFLX"]
        self.interval = interval
        self.queue = asyncio.Queue()
        self.producer = FinanceLakeKafkaProducer()
        self.executor = ThreadPoolExecutor(max_workers=len(self.symbols))
        self.last_seen = {}

    def fetch_sync(self, symbol):
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            return {
                "symbol": symbol,
                "currency": info.get("currency", "USD"),
                "exchange": info.get("exchange", "UNKNOWN"),
                "price": float(info.get("currentPrice", 0.0) or 0.0),
                "open": float(info.get("open", 0.0) or 0.0),
                "high": float(info.get("dayHigh", 0.0) or 0.0),
                "low": float(info.get("dayLow", 0.0) or 0.0),
                "volume": int(info.get("regularMarketVolume", 0) or info.get("volume", 0) or 0),
                "market_cap": int(info.get("marketCap", 0) or 0),
                "timestamp": info.get("regularMarketTime"),
                
            }
        except Exception as e:
            logging.error(f"[ERROR] Fetch {symbol}: {e}")
            return None

    async def fetch_worker(self):
        """
        Periodically fetches data for all symbols using a thread pool.
        """
        logging.info(f"Starting Yahoo async fetcher for: {self.symbols}")
        loop = asyncio.get_running_loop()
        while True:
            start_time = time.time()
            
            tasks = [
                loop.run_in_executor(self.executor, self.fetch_sync, sym)
                for sym in self.symbols
            ]
            results = await asyncio.gather(*tasks)

            for result in results:
                if result:
                    symbol = result["symbol"]
                    timestamp = result["timestamp"]
                    
                    # Deduplication
                    if self.last_seen.get(symbol) == timestamp:
                        logging.debug(f"[SKIP] Duplicate data for {symbol} at {timestamp}")
                        continue
                        
                    self.last_seen[symbol] = timestamp
                    await self.queue.put(result)
            
            elapsed = time.time() - start_time
            sleep_time = max(0, self.interval - elapsed)
            await asyncio.sleep(sleep_time)

    async def send_worker(self):
        """
        Consumes from queue and sends to Kafka.
        """
        logging.info("Starting Yahoo async sender.")
        while True:
            data = await self.queue.get()
            try:
                self.producer.send(data)
            except Exception as e:
                logging.error(f"[KAFKA ERROR] {e}")
            finally:
                self.queue.task_done()

    async def run_async(self):
        await asyncio.gather(self.fetch_worker(), self.send_worker())

    def run(self):
        try:
            asyncio.run(self.run_async())
        except KeyboardInterrupt:
            logging.info("Stopping Yahoo connector.")

if __name__ == "__main__":
    YahooFinanceConnector().run()
