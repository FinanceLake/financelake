# Kafka Ingestion Demo – FinanceLake

This directory contains a working demo of a real-time ingestion pipeline using **Apache Kafka**, as part of the FinanceLake project.

## ⚙️ Architecture

- Kafka & Zookeeper run locally using Docker Compose (defined at the project root)
- A Python producer sends messages to a Kafka topic
- A Python consumer reads those messages from the topic
- Configuration is handled via environment variables loaded with `python-dotenv` in a `config.py` file

## 🧱 Prerequisites

- Docker installed  
- Python 3 installed  
- Required packages: `kafka-python` and `python-dotenv`  
- Port `9092` available for Kafka  

```bash
pip install kafka-python python-dotenv
```

## 🚀 Getting Started

### 1. Start Kafka & Zookeeper services

From the project root, run:

```bash
docker-compose up -d
```

### 2. Check if the containers are running

```bash
docker ps
```
You should see `kafka-test-kafka-1` and `kafka-test-zookeeper-1` in the output.

### 3. Run the Kafka producer

```bash
python kafka_producer.py
```
This script sends sample messages to the Kafka topic (e.g., `test-topic`).

### 4. Run the Kafka consumer

```bash
python kafka_consumer.py
```
This script reads and displays the messages from the Kafka topic.

### 5. Stop the services

```bash
docker-compose down
```

## 📁 Project Structure

```
financeLake/
├── .env.example           # Example environment file to configure variables
├── docker-compose.yml     # Docker Compose (Kafka + Zookeeper) - at the root
└── kafka-test/
    ├── resources/         # Screenshots and demo video
    │   ├── docker_ps.png
    │   ├── kafka_producer.png
    │   └── kafka_consumer.png
    ├── config.py          # Loads environment variables
    ├── kafka_producer.py  # Kafka producer script
    ├── kafka_consumer.py  # Kafka consumer script
    └── README.md          # This file
```

## 🔐 Notes

Make sure to copy `.env.example` to `.env` and update it with your real values when testing locally. The `.env` file should **not** be committed to version control.