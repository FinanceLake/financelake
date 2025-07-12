<div align="center">
<br/>
<img src="resources/img/logo.png" width="120px" alt="">
<br/>

# FinanceLake

[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat&logo=github&color=2370ff&labelColor=454545)](http://makeapullrequest.com)
[![unit-test]()](https://github.com/FinanceLake/financelake/actions/workflows/test.yml)
[![Join us on Discord](https://img.shields.io/badge/Join_Us_on-Discord-5865F2?style=flat&logo=discord&logoColor=white&labelColor=2C2F33)](https://discord.gg/rP2dNEFJ4Y)

</div>
<br>
<div align="left">

## What is FinanceLake?
[FinanceLake](#) is an **open-source financial data platform** that ingests, analyzes, and visualizes market and financial data — similar in ambition to platforms like Bloomberg Terminal, but powered by open technologies.

Whether you're a quant, data engineer, open-source maintainer, or trading enthusiast, **FinanceLake** offers a scalable and intelligent data stack to support **real-time insights**, **financial research**, and **data-driven decision-making**.

---

## 🚀 Features

- 📥 **Data Ingestion**  
  Real-time and batch ingestion pipelines using **Apache Kafka**, **Apache NiFi**, and **API connectors** (e.g., Yahoo Finance, Alpha Vantage, Quandl, etc.)

- ⚙️ **Big Data Processing**  
  Built on top of **Apache Spark**, **Hadoop**, and **Delta Lake** for scalable and resilient analytics.

- 📈 **Advanced Analytics**  
  Analyze financial trends, compute indicators, perform backtesting, and build custom financial metrics.

- 📊 **Interactive Visualization**  
  Visual dashboards powered by **Grafana**, **Apache Superset**, or **Streamlit**.

- 🧠 **Query Engine**  
  Ask questions and get answers using a simple SQL-like interface or a natural language layer (NLQ) with optional LLM integration.

- 📡 **Data APIs**  
  REST & GraphQL APIs to expose insights and dashboards to downstream systems or external apps.

---

## 💡 Use Cases

- Market trend monitoring for trading teams
- Quantitative research and strategy testing
- Portfolio performance visualization
- Risk metrics computation
- Real-time financial data streaming and alerting

## 🎯 What can be accomplished with FinanceLake?

FinanceLake empowers users to unlock value from vast streams of financial and economic data. Here’s what you can achieve:

### 🧠 Derive Actionable Insights
- Track price movements, volatility, and trends across global markets
- Identify leading/lagging indicators to guide investment decisions
- Measure performance against custom benchmarks or indices

### 📈 Build & Test Trading Strategies
- Backtest strategies using historical tick/ohlcv data
- Generate buy/sell signals using technical indicators (RSI, MACD, EMA…)
- Evaluate drawdown, Sharpe ratio, beta, and other risk metrics

### 📊 Visualize and Monitor in Real Time
- Build dynamic dashboards to monitor positions, portfolios, and KPIs
- Stream live feeds for asset prices, news sentiment, or macro indicators
- Trigger alerts on thresholds or anomalies (via webhook, email, Slack)

### 🔎 Query Like a Pro
- Explore structured and unstructured financial data using SQL or natural language
- Query fundamentals, earnings, economic events, ESG scores, and more
- Slice and dice data per sector, geography, or custom segments

### 🏗️ Build Custom Financial Applications
- Create custom dashboards for hedge funds, fintech apps, or research teams
- Feed data into machine learning pipelines (e.g., predictive models)
- Connect external systems (trading bots, ML models, BI tools) via API

### 🧩 Extend and Contribute
- Add custom connectors to new data sources (e.g., crypto exchanges, alt-data)
- Contribute notebooks, indicators, or data visualizations
- Help shape the roadmap of an open, transparent financial platfor

## 👉 Live Demos

Comming soon !!

## 💪 Supported Data Sources

Comming soon !!

## 🚀 Getting Started

### Installation
You can set up  FinanceLake by following our step-by-step instructions for either Docker Compose or Helm. Feel free to [ask the community](#💙-community) if you get stuck at any point.

- [Install via Docker Compose](#)
- [Install via Helm](#)

## 🤓 Usage

Please see [detailed usage instructions](#). Here's an overview on how to get started using FinanceLake.


## Contributing
Please read the [contribution guidelines](#) before you make contribution. The following docs list the resources you might need to know after you decided to make contribution.

- [Create an Issue](#): Report a bug or feature request to FinanceLake
- [Submit a PR](#): Start with [good first issues](#) or [issues with no assignees](#)
- [Join Mailing list](#): Initiate or participate in project discussions on the mailing list
- [Write a Blog](#): Write a blog to share your use cases about FinanceLake
- [Develop a Plugin](#):  Integrate FinanceLake with more data sources as [requested by the community](#)

### 👩🏾‍💻 Contributing Code

If you plan to contribute code to FinanceLake, we have instructions on how to get started with setting up your Development environemtn.

- [Developer Setup Instructions](#)
- [Development Workflow](#)


### 📄 Contributing Documentation

One of the best ways to get started contributing is by improving FinanceLake's documentation. 

- FinanceLake's documentation is hosted at [FinanceLake](#)
- **We have a separate GitHub repository for FinanceLack's documentation:** [github.com/FinanceLake/financelake-docs](https://github.com/FinanceLake/financelake-docs)

## ⌚ Roadmap

- <a href="#" target="_blank">Roadmap</a>: Detailed roadmaps for FinanceLake.

## 💙 Community

Message us on <a href="https://discord.gg/rP2dNEFJ4Y" target="_blank">Discord</a>

## 📄 License<a id="license"></a>



## 🔧 Environment Configuration

Before running the project, configure your environment variables.

1. Copy the `.env.example` file and create your own `.env` file:

```bash
cp .env.example .env
```
2. Edit the .env file and fill in your specific configuration:

-DB_HOST:		Database host (e.g., localhost)
-KAFKA_BROKER: 		Kafka broker address
-SPARK_MASTER		Spark master URL
-API_KEY:		Your API key
-DATA_SOURCE_URL:	URL to fetch data from
-RAW_DATA_PATH:		Path for storing raw data
-DASHBOARD_USER:	Dashboard login user
-LOG_LEVEL:		Logging level (e.g., INFO, DEBUG)


# Steps to execute Kafka & HDFS:

1. Start Kafka & Zookeeper

bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

2. Start HDFS 

Make sure HDFS is running on localhost:9000.

3. Start the Kafka producer

python producer.py

4. Deploy the HDFS connector

curl -X POST -H "Content-Type: application/json" --data @hdfs-sink.json http://localhost:8083/connectors

## 📊 Dashboard Setup (Apache Superset)

### Prerequisites
- Docker and Docker Compose installed
- Stock data processed and available in `data/processed/` directory

### Quick Start

1. **Prepare the data for Superset:**
   ```bash
   cd financelake
   python scripts/prepare_data_for_superset.py
