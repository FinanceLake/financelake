# Streaming Ingestion – FinanceLake

This module implements the **real-time (streaming) data ingestion pipeline** for FinanceLake.

## What does the code do?

- **Fetches real-time financial data** for a dynamic list of NASDAQ symbols using the Alpha Vantage API.
- **Loads the symbol list automatically** from the official NASDAQ listings (always up-to-date).
- **Handles API rate limits and errors**: Adaptive sleep, 2-minute pause on rate limit, and detailed logging.
- **Sends data to Kafka**: Each record is serialized as JSON and sent to a configurable Kafka topic.
- **Modular structure**: Symbol loading, data fetching, and Kafka logic are separated for maintainability.

## Key Features

- **Dynamic symbol management**: No hardcoded list; always current with the market.
- **Production-ready error handling**: Graceful handling of API/network issues.
- **Scalable**: Can be extended to other APIs or more symbols easily.

## How to use

1. **Configure your Alpha Vantage API key** in `producer.py` (replace `'YOUR_API_KEY'`).
2. **Start Kafka and Zookeeper** (e.g., with Docker Compose).
3. **Run the producer** from the project root:
   ```bash
   python3 data_ingestion/streaming/producer.py
   ```
4. **Monitor**: Check logs for progress and errors. Data will be sent to the configured Kafka topic.

## Files

- `producer.py` : Main streaming producer script.
- `symbol_loader.py` : Loads the NASDAQ symbol list dynamically.
- `config.py` : Configuration for Kafka and other settings.

---

For more details, see the global README in `data_ingestion/`.
