# FinanceLake – Spark, Delta Lake, ML & Reinforcement Learning

This project implements a **streaming financial data processing pipeline** based on **Apache Spark**, **Delta Lake**, **Machine Learning**, and **Reinforcement Learning**.

It consumes stock market data from **Kafka**, structures it according to the **Bronze → Silver → Gold** architecture, trains a **classification** model, and simulates a **trading strategy using reinforcement learning**.
---

## Features

- Real-time data ingestion from Kafka
- **Data Lakehouse** architecture with Delta Lake:
  - Bronze : raw ingestion
  - Silver : cleaning and enrichment
  - Gold : analytical aggregations
- Transactional storage using **Delta Lake**
- Training of a **Machine Learning model (Logistic Regression)**
- Evaluation via **AUC**
- Trading simulation using **Reinforcement Learning (Q-learning)**
- Continuous pipeline execution
- Docker support

---

## Pipeline Architecture

Kafka
↓
Bronze (Delta)
↓
Silver (Delta)
↓
Gold (Delta)
↓
Machine Learning
↓
Reinforcement Learning

---

## Project Structure
.
├── Dockerfile
├── README.md
└── stock_streaming.py


---

## Processed Data

Data consumed from Kafka has the following format:

```json
{
  "symbol": "AAPL",
  "price": 182.45,
  "volume": 532145,
  "open": 180.12,
  "high": 184.01,
  "low": 179.85,
  "timestamp": "2026-01-07T15:42:10.123456"
}
```
--- 

## Requirements

- Docker
- Kafka is assumed to be already running in the Docker network finance-net.

--- 

## Installation

The required Python dependencies are installed directly inside the Spark container.

1. Access the Spark Master container:

```bash
docker exec -it --user root spark-master bash
```

2. Install system and Python dependencies:

```bash
apt-get install -y python3-pip python3-dev gcc g++
pip3 install numpy pandas scikit-learn
```
---

## Execution

Run the Spark pipeline from the Spark Master container:

```bash
docker exec -it --user root spark-master bash

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  /app/stock_streaming.py
```
---
## Pipeline Workflow

- Continuous ingestion of Kafka data into Bronze
- Cleaning and enrichment into Silver
- Analytical aggregations into Gold
- Every 60 seconds :
  - Train the ML model
  - Compute AUC
  - Train the RL model
  - Update the portfolio value
- Graceful shutdown with CTRL + C

---

## Machine Learning

- Model: Logistic Regression
- Features :
  - Average price
  - Volatility
  - Total volume
- Label :
  - Up (1) / Down (0)
- Metric: AUC

---

## Reinforcement Learning

- Algorithm: Q-learning
- Actions :
  - -1 : SELL
  - 0 : HOLD
  - 1 : BUY
- Realistic trading management:
  - Capital initial
  - Position size
  - Transaction fees
- Results stored in Delta (gold_rl)

---

## Technologies Used

- Apache Spark 3.5
- Delta Lake
- Kafka
- Python
- Docker
- PySpark ML









