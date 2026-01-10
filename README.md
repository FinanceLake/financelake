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
[FinanceLake](#) is an **open-source financial data platform** that allows you to ingest, process, analyze, and visualize real-time stock market data.

It includes two main components in this project:
- Yahoo Finance Kafka Producer : retrieves stock prices in real-time and sends them to Kafka.
- Delta Lake Stock Analyzer : a Spark streaming pipeline that processes Kafka data through Bronze → Silver → Gold layers, trains ML models, and simulates trading strategies using reinforcement learning.

FinanceLake provides a scalable, intelligent platform for **real-time financial insights**, **quantitative research**, and **data-driven decision-making**.
---

## 🚀 Features

- **Kafka Producer**
  - Real-time stock price ingestion from Yahoo Finance
  - Automatic fallback to simulated data if real data fails
  - Sends JSON data to Kafka topic stock_prices
  - Tracks multiple symbols: AAPL, GOOGL, MSFT, AMZN, TSLA, META, NFLX, NVDA
  - Docker-ready for easy deployment
  - [See detailed README](./producer/README.md)

- **Delta Lake Stock Analyzer**
  - Data Lakehouse architecture (Bronze → Silver → Gold)
  - Bronze: raw Kafka ingestion
  - Silver: data cleaning and enrichment
  - Gold: aggregations for ML and RL
  - Transactional storage using Delta Lake
  - Machine Learning (Logistic Regression) for trend prediction
  - Reinforcement Learning (Q-learning) for portfolio simulation
  - Continuous pipeline execution via Spark Streaming
  - [See detailed README](./spark/README.md)

---

## Requirements
- Docker

## Installation & Setup
Start the project using Docker:

```bash
# Start all Docker containers
docker compose up -d

# Check running containers
docker ps

# Create a Docker network for FinanceLake (if not already created)
docker network create finance-net

# Connect Kafka and Zookeeper containers to the network
docker network connect finance-net kafka
docker network connect finance-net zookeeper
```

## Pipeline Workflow
- Continuous ingestion of Kafka data into Bronze layer
- Cleaning and enrichment into Silver layer
- Analytical aggregations into Gold layer
- Every 60 seconds:
  - Train the ML model and compute AUC
  - Train the RL model and update portfolio value
- Graceful shutdown via CTRL + C


## Contributing
Please read the [contribution guidelines](#) before you make contribution. The following docs list the resources you might need to know after you decided to make contribution.

- [Create an Issue](#): Report a bug or feature request to FinanceLake
- [Submit a PR](#): Start with [good first issues](#) or [issues with no assignees](#)
- [Join Mailing list](#): Initiate or participate in project discussions on the mailing list
- [Write a Blog](#): Write a blog to share your use cases about FinanceLake
- [Develop a Plugin](#):  Integrate FinanceLake with more data sources as [requested by the community](#)

### 👩🏾‍💻 Contributing Code

If you plan to contribute code to FinanceLake, we have instructions on how to get started with setting up your Development environment.

- [Developer Setup Instructions](#)environment
- [Development Workflow](#)


### 📄 Contributing Documentation

One of the best ways to get started contributing is by improving FinanceLake's documentation. 

- FinanceLake's documentation is hosted at [FinanceLake](#)
- **We have a separate GitHub repository for FinanceLake's documentation:** [github.com/FinanceLake/financelake-docs](https://github.com/FinanceLake/financelake-docs)

## ⌚ Roadmap

- <a href="#" target="_blank">Roadmap</a>: Detailed roadmaps for FinanceLake.

## 💙 Community

Message us on <a href="https://discord.gg/rP2dNEFJ4Y" target="_blank">Discord</a>

## 📄 License<a id="license"></a>
