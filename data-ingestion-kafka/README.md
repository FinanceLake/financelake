###                                               FinanceLake – Mini Data Ingestion Project

Objective

The goal of this project is to implement a real-time data ingestion architecture by simulating financial data and storing it in a Data Lake (Delta Lake) for future analysis.

Proposed Architecture

![Architecture du projet](../resources/img/architecture.png)

This architecture follows a modern, streaming-oriented approach to ensure fast, reliable, and scalable ingestion.

**Justification for Technological Choices
    *Apache Kafka: Used as a message broker to transport real-time events. It is a scalable, decentralized, and highly performant system.
    -Spark Structured Streaming: Enables consumption of data from Kafka, transformation, and continuous storage. Its native compatibility with Kafka and -Delta Lake makes it an excellent choice for stream processing.
    -Delta Lake: Provides a transactional (ACID) storage layer on top of Parquet files. It ensures reliable and versioned data management within a Data Lake.
    -Simulated Python Producer: A simple data source to simulate a continuous stream of stock prices.
*
How It Works

1. Start Kafka (Assumed to be pre-configured)

Start Zookeeper and then Kafka:
# bin/zookeeper-server-start.sh config/zookeeper.properties
# bin/kafka-server-start.sh config/server.properties
Create the topic "finance":
# bin/kafka-topics.sh --create --topic finance --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

2. Run the Producer
# cd ingestion-kafka/producer
# python3 producer.py

This sends messages like:
> {"symbol": "AAPL", "price": 174.5}

3. Launch Spark Structured Streaming
# cd ingestion-kafka/spark
# spark-submit kafka_to_delta.py
The data will be stored in data/finance_delta/ in Delta Lake format.


🚀 Author

Name: Amina ELBAYYADI
Program: Master’s in Big Data & Artificial Intelligence