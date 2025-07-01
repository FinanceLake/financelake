# Project Architecture

## Overview
Here is a diagram of our workflow :

![Workflow Diagram](https://github.com/user-attachments/assets/32005cbb-2aa5-4104-9a88-99b23f319b99)

**Context** : 
As members of the first year class of BigData and AI Masters, we organised a couple of online reunions to discuss the architecture that this Finance Lake project will follow 
and came to the decision of adopting a Lambda Architecture (which is characterised with a dual-pipeline, one for batch ingestion and processing and the other for streaming/real-time ingestion and processing).
So far, we agreed on the components for the data sources, the ingestion, the intermediate storage and the processing phases. 

## Components
- **Data Sources** : We decided to use Alpha Vantage API and/or Yahoo Finance API as Data sources for our Finance Lake project.
- **Data Ingestion** : Since we're adopting a Lambda Architecture, we decided to use Apache Kafka for stream ingestion and Apache Nifi for batch ingestion.
- **Intermediate Storage** : This phase is mainly about storing the ingested data before processing it for better tolerance to process failure. And we came to the conclusion of using two different Kafka topics, one for streaming data and the other for batch data.
- **Processing** : For processing, we agreed to go for real-time processing only since this is a Finance Lake project and we figured most analytical processes will be real-time processes.
- **Data Storage** : (WIP). We haven't decided on which storage system to use since it will eventually depend on the results we get from the processing phase, so it's either going to be a Data Warehouse for analytics and reports, a Data Lake for raw data or a Relational Database for structured data.
- **Analytics** : (WIP). In this phase, we're talking about using analytical engines like Apache Spark or Presto/Trino, Machine Learning like Scikit-learn or TensorFlow and Visualisation like PowerBI and Tableau.
- **APIs** : (WIP). This phase is about Exposition APIs like Fast API or Spring Boot for RESTful APIs and GraphQL for flexible queries.
- **FrontEnd** : (WIP). A graphical user interface for the project's application using React.js, javascript or Dash.

*WIP : Work In Progress*
## Choice Explanation
### Lambda Architecture
The Lambda Architecture is particularly well-suited for finance lake projects due to its ability to handle both real-time analytics and historical batch processing 
while ensuring fault tolerance and scalability. It can handle **High-Velocity Market Data**; it balances **Accuracy** and **Latency**.

### Alpha Vantage API
Alpha Vantage is a financial data API that provides real-time and historical stock, forex (FX), cryptocurrency, and economic data for developers, traders, and analysts. 
It offers both free and premium tiers, making it popular for algorithmic trading, research, and financial apps.

### Yahoo Finance API
Yahoo Finance offers one of the most popular free sources of financial market data, though it's important to note that Yahoo no longer officially supports its public API.
We can still use the unofficial methods of this API through the python library 'yfinance'.

### Apache Kafka
Apache Kafka is a distributed event streaming platform used by thousands of companies for high-performance data pipelines, streaming analytics, and real-time applications.

**Data Flow**

Producers → Kafka Cluster (Brokers + Topics) → Consumers

**Apache Kafka's advantages** :

- **High Throughput** : Millions of messages per second
- **Scalability** : Add brokers to scale horizontally
- **Durability** : Messages persisted to disk with replication
- **Low Latency** : Near real-time delivery (~10ms)
- **Fault Tolerance** : Replication protects against node failures

### Apache Nifi
Apache NiFi excels at batch data ingestion with its visual workflow design, robust error handling, and built-in processors.

**Batch Ingestion Architecture in NiFi**

[Batch Source] → [NiFi Flow] → [Kafka Topic for batch]
      ↑
[Scheduled Trigger]

NiFi's strength lies in flexible batch workflows with minimal code. For recurring batch jobs with monitoring needs, it's an excellent choice.

### Spark Streaming
Spark Streaming is an extension of Apache Spark that enables scalable, fault-tolerant stream processing of live data streams. 
It ingests data in mini-batches and processes them using Spark's distributed computing engine.

**How Spark Streaming Works**
- Breaks streams into small batches (e.g., every 1 second)
- Processes each batch as a micro-RDD (Resilient Distributed Dataset)
- Unified API for batch + streaming (same code for both)

The most popular supported data source for Spark Streaming is Apache Kafka.
