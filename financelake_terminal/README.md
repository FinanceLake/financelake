# FinanceLake Terminal

<div align="center">
<br/>
<img src="../resources/img/logo.png" width="120px" alt="">
<br/>
</div>

## Overview

**FinanceLake** Terminal is a modern, real-time financial dashboard built on open-source technologies. It processes live market data, applies machine learning for price direction prediction, and visualizes complex technical indicators in a professional "Terminal-like" interface.

The project implements a Lakehouse architecture using **Delta Lake**, **Apache Spark**, **Kafka**, and **Streamlit**.

### Architecture

The platform follows the **Medallion Architecture**:

1. **Ingestion (Kafka)**: Fetches live WebSocket data from Finnhub (Stocks/Forex).
2. **Bronze Layer (Delta)**: Raw historical dump of streaming data.
3. **Silver Layer (Delta)**: Cleaned, parsed, and typed data.
4. **Gold Layer (Delta)**: Aggregated features (Windows), technical indicators, and ML Predictions.
5. **Serving (Streamlit)**: A low-latency UI that reads directly from the Gold layer.

## Key Features

* **Real-Time Streaming**: Sub-second latency data ingestion via WebSocket & Kafka.
* **AI-Powered Analysis**: Integrated **Random Forest Classifier** (Spark ML) predicting short-term price movements with confidence scores.
* **Technical Indicators**:
  * **MACD** (Moving Average Convergence Divergence)
  * **Bollinger Bands** (Volatility measurement)
  * **OBV** (On-Balance Volume)
  * **Stochastic Oscillator**
* **Delta Lake Storage**: ACID transactions and scalable storage for streaming data.
* **Dark/Light Mode UI**: Fully responsive Streamlit dashboard.

## Installation & Setup

### Prerequisites

* Python 3.10+
* Apache Kafka (Local or Docker)
* Apache Spark 3.5+ (with Delta Lake support)
* Java 8/11 (for Spark)
* Jupyter Notebook or VS Code (to run `.ipynb` files)

### 1. Clone & Environment

```bash
git clone [https://github.com/FinanceLake/financelake.git](https://github.com/FinanceLake/financelake.git)
cd financelake_terminal
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

```

### 2. Configuration

Copy the included environment template:

```ini
cp .env.example .env
```

Edit the `.env` file with your specific configuration:

```ini
# .env config
BASE_PATH=/tmp/financelake
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=store_prices
FINNHUB_API_KEY=your_finnhub_api_key_here

```

### 3. Start Infrastructure (Kafka & Zookeeper)

You can run the infrastructure using **Locally** OR **Docker Compose**.

#### Option A: Local Installation

If you have downloaded Apache Kafka locally, run the following commands in separate terminals from your Kafka installation directory:

1. **Start Zookeeper:**

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties

```

2. **Start Kafka Broker:**

```bash
bin/kafka-server-start.sh config/server.properties

```

#### Option B: Docker Compose

```bash
docker-compose up -d zookeeper kafka

```

---

## Running the Pipeline

The system consists of independent components that must run simultaneously.

### Step 0: Initial Data Collection & Model Training ⚠️

**Important:** The AI model requires historical data to function. When running for the very first time, the system cannot predict anything because the model file doesn't exist yet.

1. **Collect Data:** Run the **Producer** (**Step 1**) for 10-20 minutes to collect initial data into the Bronze/Silver layers.
2. **Train Model:** Open `model.ipynb` in Jupyter/VS Code. Run all cells to:

* Read the collected history.
* Train the Random Forest Classifier.
* Save the model to your `MODEL_PATH`.

3. *Once the model is trained and saved, you can proceed to Step 2.*

### Step 1: Data Producer

Starts fetching data from Finnhub and pushing to Kafka.

* **Action:** Open `producer.ipynb` in Jupyter/VS Code and run all cells.
* **Status:** This notebook must keep running to generate the data stream.

### Step 2: Stream Processor (ETL + AI)

This Spark job manages the core **Medallion Architecture** flow (**Bronze**  **Silver**  **Gold**) and runs live inference.

* **Action:** Open `streaming_processor.ipynb` in Jupyter/VS Code.
* **⚠️ CRITICAL: Run Interactively (Cell by Cell)**
  You must verify that data exists in each layer before starting the next stream to avoid errors.

1. **Start Bronze Stream:** Run the Bronze cells. Check your data folder (`/tmp/financelake/bronze`). **Wait** until folders and parquet files appear.
2. **Start Silver Stream:** Run the Silver cells. **Wait** until the `silver` folder is created and populated.
3. **Start Gold Stream:** Only after Silver is running and has data, run the Gold cells to start aggregations and predictions.

* **Status:** This notebook must keep running to process data in real-time.

### Step 3: The Terminal (UI)

Launches the web interface to visualize the Gold layer data.

```bash
streamlit run dashboard.py

```

> Access the dashboard at **http://localhost:8501**

---

## Machine Learning Model

The project uses a **Random Forest Classifier** trained on historical windowed features.

* **Features**: `avg_price`, `avg_volume`, `volatility` (Standard Deviation).
* **Target**: `next_price_direction` (1 if price goes up, 0 if down).

**Re-training** (optionally)**:**
As you collect more data over time, re-run `model.ipynb` periodically to update the model with the latest market trends.

## 📂 Project Structure

```
financelake_terminal/
├── .env.example               # Environment configuration template
├── config.py                  # Central configuration
├── dashboard.py               # Streamlit Visualization App
├── producer.ipynb             # Kafka Producer Notebook 
├── streaming_processor.ipynb  # Spark Streaming Notebook 
├── model.ipynb                # ML Model Training Notebook
├── requirements.txt           # Python Dependencies
└── README.md                  # Project Documentation

```

## 🤝 Contribution

Contributions are welcome! Please submit a PR following the standard template.

1. Fork the repository.
2. Create a Feature branch (`git checkout -b feature/AmazingFeature`).
3. Commit changes (`git commit -m 'Add AmazingFeature'`).
4. Push to the branch (`git push origin feature/AmazingFeature`).
5. Open a Pull Request.

## 📄 License

Distributed under the MIT License.

**Developed by Big Data & AI Class 2025** | *Université Ibn Zohr*

```

```
