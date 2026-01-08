# Yahoo Finance Kafka Producer 

This project implements a **Kafka producer in Python** that retrieves real-time stock market data from **Yahoo Finance** and publishes it to a Kafka topic (`stock_prices`).

When real data is not available, the producer automatically generates **reliable simulated data** to ensure continuous data streaming.
---

## Features

- Retrieval of **real stock prices** using `yfinance`
- Data publishing to **Apache Kafka**
- Error handling with **automatic fallback**
- **Complete and consistent** message structure
- Docker support
- Tracking of multiple stock symbols:
  - `AAPL`, `GOOGL`, `MSFT`, `AMZN`, `TSLA`, `META`, `NFLX`, `NVDA`

---

## Project Structure
.
├── Dockerfile
├── producer.py
├── README.md
└── requirements.txt

---

## Produced Data (JSON format)

Each message sent to the Kafka topic follows this structure :

```json
{
  "symbol": "AAPL",
  "price": 182.45,
  "volume": 532145,
  "open": 180.12,
  "high": 184.01,
  "low": 179.85,
  "timestamp": "2026-01-07T15:42:10.123456",
  "price_variation": 1.29,
  "price_increase": 1
}
```

Key fields : 
- symbol : stock ticker symbol
- price : current price
- volume : trading volume
- open, high, low : opening, highest, and lowest prices
- timestamp : ISO-formatted date and time
- price_variation : price variation in %
- price_increase : 1 if price increased, 0 otherwise

## Requirements

- Python 3.10+
- Docker

- Kafka is assumed to already be running within the Docker network finance-net.

## Usage with Docker

- Build the image

```bash
cd producer
docker build -t stock-producer .
```

- Run the container

```bash
docker run --rm --network finance-net stock-producer
```
---
## How It Works

- Data is sent every 2 seconds per symbol
- 30-second pause between each full cycle
- If Yahoo Finance fails:
  - automatic switch to simulated data
- Graceful shutdown with CTRL + C
---

## Kafka Topic

- Topic name : stock_prices
- Data format : JSON
- Producer : YahooFinanceProducer