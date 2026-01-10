# 📊 FinanceLake – Real-Time Data Ingestion & Analytics Platform

FinanceLake is an **end-to-end Big Data Lakehouse project** designed for **real-time financial data ingestion, processing, analytics, and visualization**.

The platform leverages **Apache Kafka, Apache Spark, Delta Lake, and Streamlit**, following a **Bronze / Silver / Gold** Lakehouse architecture, and integrates a **machine learning module** for next-day price prediction.

## 🖼️ Global Architecture

![FinanceLake Architecture](..resources/images/architecture.PNG)

## 🏗️ Architecture Overview

FinanceLake follows a **modern Lakehouse architecture**:

- **Kafka** handles real-time data streaming
- **Spark Structured Streaming** processes incoming data
- **Delta Lake** ensures ACID transactions and reliability
- **Streamlit Dashboard** monitors ingestion and analytics
- **Machine Learning models** predict next-day prices

---

## 🔄 Data Flow

1. Financial data is collected from **Yahoo Finance API**
2. Data is published to **Kafka topics**
3. **Spark Streaming** consumes Kafka data
4. Data is stored in **Delta Lake (Bronze, Silver, Gold layers)**
5. Dashboard displays ingestion metrics and analytics
6. ML models use Gold data for predictions

---

## 🧠 Lakehouse Layers

### 🟫 Bronze Layer (Raw Data)
- Raw streaming data
- Minimal transformation
- Stored as Delta tables

**Fields:**
- `ticker`
- `price`
- `volume`
- `ingest_time`

---

### 🪙 Silver Layer (Cleaned Data)
- Data validation
- Type casting
- Null value handling
- Schema enforcement

---

### 🥇 Gold Layer (Analytics)
- Aggregated daily metrics
- Used by:
  - Dashboard
  - Machine Learning models

**Fields:**
- `trading_date`
- `ticker`
- `avg_price`
- `total_volume`

---

## 📊 Streamlit Dashboard Features

- 📅 Last ingestion timestamp
- 📦 Total ingested records
- ⏱ Ingestion health status
- 📈 Ingestion volume per day
- 🧾 Latest Bronze events
- 🧾 Latest Gold events
- 🚨 Error logs (Delta table)
- 🔮 Price prediction per ticker
- 🌙 Dark / Light mode
- 🔄 Manual refresh

---

## 🤖 Machine Learning Module

- **Model type:** LSTM
- **Framework:** TensorFlow / Keras
- **Input features:**
  - Average price
  - Total volume
- **Window size:** 7 days
- **Output:** Next-day price prediction
- **One model per ticker**

---

## 🛠️ Technologies Used

| Category | Technology |
|--------|-----------|
| Programming | Python 3.10 |
| Streaming | Apache Kafka |
| Processing | Apache Spark |
| Storage | Delta Lake hdfs |
| Dashboard | Streamlit |
| Machine Learning | TensorFlow, Keras |
| Visualization | Matplotlib |
| Data Source | Yahoo Finance |
| Architecture | Lakehouse |

---

## 📁 Project Structure

financelake/
│
├── producer/
│ └── kafka_producer.py
│
├── pipeline/
│ ├── bronze_stream.py
│ ├── bronze_to_silver.py
│ ├── silver_stream_merge.py
│ └── silver_to_gold.py
│
├── visualization/
│ └── dashboard.py
│
├── analytics/
│ └── models/
│ ├── model_AAPL.h5
│ └── scaler_AAPL.gz
│
├── config/
│ └── spark_config.py
│
├── assets/
│ ├── financelake-logo.jpg
│ └── architecture.png
│
└── README.md


---

## ▶️ How to Run the Project 

### 1️⃣ Prerequisites

- Python 3.10
- Java 8 or 11
- Apache Kafka
- Apache Spark
- pip packages:
  ```bash
  pip install streamlit pyspark delta-spark kafka-python yfinance tensorflow joblib matplotlib pandas

2️⃣ Start Kafka

```bash
kafka-server-start.bat .\config\kraft\server.properties


3️⃣ Create Kafka Topic

kafka-topics.bat --create ^
--topic finance-data ^
--bootstrap-server localhost:9092

4️⃣ Run Kafka Producer

python producer/kafka_producer.py

5️⃣ Run Spark Pipelines

Bronze ingestion
spark-submit --packages io.delta:delta-core_2.12:1.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pipeline/bronze_stream.py

Bronze → Silver

spark-submit --packages io.delta:delta-core_2.12:1.0.0 pipeline/bronze_to_silver.py

Silver merge

spark-submit --packages io.delta:delta-core_2.12:1.0.0 pipeline/silver_stream_merge.py

Silver → Gold

spark-submit --packages io.delta:delta-core_2.12:1.0.0 pipeline/silver_to_gold.py

6️⃣ Launch the Dashboard

streamlit run visualization/dashboard.py


