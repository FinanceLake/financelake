"""
Alpha Vantage Connector for FinanceLake
---------------------------------------
Fetches stock market data using the Alpha Vantage API and sends it to Kafka.
Refactored to use asyncio and aiohttp for non-blocking execution.
"""

import os
import asyncio
import logging
import json
import time
from datetime import datetime
import aiohttp
from ingestion.kafka_producer import FinanceLakeKafkaProducer

# Configure logging
logging.basicConfig(
    filename='logs/alphavantage_connector.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

ALPHAVANTAGE_API_KEY = "YOUR API KEY HERE"

class AlphaVantageConnector:
    """
    Collects stock price data using the Alpha Vantage API and publishes it to Kafka
    using an asynchronous Producer-Consumer pattern.
    """

    def __init__(self, api_key, symbols=None, interval="1min", fetch_delay=60):
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"
        self.symbols = symbols or ["AAPL", "GOOG", "MSFT", "AMZN", "TSLA"]
        self.interval = interval
        self.fetch_delay = fetch_delay
        
        # Internal async queue for producer-consumer pattern
        self.queue = asyncio.Queue()
        
        # State tracking for deduplication
        self.last_seen = {}

        self.producer = FinanceLakeKafkaProducer(
            topic="stock_prices"
        )

        if not self.api_key:
            raise ValueError("Missing Alpha Vantage API key.")

    async def fetch_symbol_data(self, session, symbol):
        """
        Worker 1 (Fetcher): Asynchronously fetches data from the API.
        """
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": self.interval,
            "apikey": self.api_key,
        }

        try:
            async with session.get(self.base_url, params=params, timeout=15) as response:
                if response.status != 200:
                    logging.error(f"[ERROR] API returned {response.status} for {symbol}")
                    return None
                
                data = await response.json()
                
                # Alpha Vantage returns nested dicts
                time_series = data.get(f"Time Series ({self.interval})", {})
                if not time_series:
                    logging.warning(f"[WARN] No data returned for {symbol}.")
                    return None

                # Get the latest timestamp
                latest_ts = sorted(time_series.keys())[-1]
                
                # Deduplication logic
                last_ts = self.last_seen.get(symbol)
                if last_ts == latest_ts:
                    logging.debug(f"[SKIP] Duplicate data for {symbol} at {latest_ts}")
                    return None
                
                # Update state
                self.last_seen[symbol] = latest_ts
                
                values = time_series[latest_ts]

                record = {
                    "symbol": symbol,
                    "price": float(values["4. close"]),
                    "open": float(values["1. open"]),
                    "high": float(values["2. high"]),
                    "low": float(values["3. low"]),
                    "volume": int(values["5. volume"]),
                    "timestamp": datetime.strptime(latest_ts, "%Y-%m-%d %H:%M:%S").isoformat(),
                    "source": "alpha_vantage",
                }
                return record

        except asyncio.TimeoutError:
             logging.error(f"[TIMEOUT] Fetching {symbol} timed out.")
             return None
        except Exception as e:
            logging.error(f"[ERROR] Fetching {symbol} failed: {e}")
            return None

    async def producer_worker(self):
        """
        Fetcher Loop: Periodically fetches data for all symbols and puts to queue.
        """
        logging.info(f"Alpha Vantage async producer started for {self.symbols}")
        
        async with aiohttp.ClientSession() as session:
            while True:
                start_time = time.time()
                
                # Create fetch tasks for all symbols concurrently
                tasks = [self.fetch_symbol_data(session, symbol) for symbol in self.symbols]
                results = await asyncio.gather(*tasks)

                for result in results:
                    if result:
                        await self.queue.put(result)
                
                # Calculate sleep time to respect rate limits/interval
                elapsed = time.time() - start_time
                sleep_time = max(0, self.fetch_delay - elapsed)
                logging.info(f"[CYCLE] Fetched {len(results)} symbols. Sleeping for {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)

    async def consumer_worker(self):
        """
        Worker 2 (Sender): Pulls from queue and sends to Kafka.
        """
        logging.info("Kafka async consumer started.")
        while True:
            record = await self.queue.get()
            try:
                # Assuming producer.send is synchronous but fast (librdkafka). 
                # If truly blocking, run_in_executor could be used, but usually fine for high throughput.
                self.producer.send(record)
                # logging.debug(f"[PUSHED] {record['symbol']} at {record['timestamp']}")
            except Exception as e:
                logging.error(f"[KAFKA ERROR] Failed to send {record}: {e}")
            finally:
                self.queue.task_done()

    async def run_async(self):
        """
        Orchestrates the asyncio event loop.
        """
        # Run fetcher and sender concurrently
        producer_task = asyncio.create_task(self.producer_worker())
        consumer_task = asyncio.create_task(self.consumer_worker())
        
        await asyncio.gather(producer_task, consumer_task)

    def run(self):
        """
        Entry point to start the async loop.
        """
        try:
            asyncio.run(self.run_async())
        except KeyboardInterrupt:
            logging.info("Stopping connector...")

if __name__ == "__main__":
    connector = AlphaVantageConnector(
        api_key=os.getenv("ALPHA_VANTAGE_KEY", ALPHAVANTAGE_API_KEY),
        symbols=["TSLA", "NFLX", "FB"],
        interval="1min",
        fetch_delay=10, 
    )
    connector.run()
