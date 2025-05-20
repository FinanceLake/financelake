# Spark Kafka Consumer to Parquet

This project implements a Spark Structured Streaming job that consumes real-time stock data from a Kafka topic and writes it into Parquet format.

## ✅ Features

- Reads from Kafka topic `stock-data`
- Parses JSON messages with fields: `symbol`, `price`, `timestamp`
- Writes output to Parquet files locally
- Supports checkpointing for fault tolerance
- Trigger interval: every 10 seconds

## 🚀 How to Run

### 1. Start Kafka Broker & Topic

Ensure Kafka is running and topic `stock-data` is created.  
Example:

```bash
kafka-topics.sh --create --topic stock-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
