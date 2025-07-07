# Data Ingestion – FinanceLake

This module implements a robust, modular data ingestion layer for the FinanceLake project, supporting both **streaming** and **batch** pipelines.

## Overview

The data_ingestion layer is designed to:

- Ingest real-time financial data from public APIs (e.g., Alpha Vantage) using a streaming pipeline.
- Ingest historical or bulk data from files (CSV, Parquet) using a batch pipeline orchestrated by Apache NiFi.
- Dynamically manage the list of symbols to ingest.
- Ensure reliability, scalability, and maintainability through clear separation of concerns and error handling.

---

## Architecture

```
+-------------------+        +-------------------+        +-------------------+
|   Data Sources    |        |   Ingestion Layer |        |   Storage Layer   |
|-------------------|        |-------------------|        |-------------------|
| - Alpha Vantage   | -----> | - Kafka Producer  | -----> | - Parquet/Delta   |
| - CSV/Parquet     | -----> | - NiFi Batch      | -----> | - Partitioned     |
+-------------------+        +-------------------+        +-------------------+
```

- **Streaming**: Real-time data is fetched from APIs and sent to Kafka topics.
- **Batch**: NiFi monitors a directory, processes new files, and sends records to Kafka.
- Both pipelines are designed to be extensible and production-ready.

---

## Streaming Pipeline

- **Producer in Python**: Fetches data for a dynamic list of symbols from Alpha Vantage (or other APIs).
- **Dynamic Symbol Loading**: The list of symbols is loaded automatically from public sources.
- **Rate Limiting & Error Handling**: The producer handles API rate limits and network errors gracefully, with adaptive sleep and logging.
- **Kafka Integration**: Data is serialized as JSON and sent to a configurable Kafka topic for downstream processing.
- **Modular Code**: Clear separation between symbol loading, data fetching, and Kafka logic.

## Batch Pipeline

- **Apache NiFi**: Orchestrates the ingestion of batch files (CSV, Parquet) from a monitored directory.
- **Automated Processing**: Each new file is detected, parsed, transformed, and its records are sent to Kafka.
- **No Redundant Python Scripts**: All batch logic is centralized in NiFi for maintainability and monitoring.
- **Docker Integration**: Volumes are configured so NiFi can access files on the host system.
- **Extensible**: Easy to add new file formats or processing steps in NiFi.

---

## Key Features & Choices

- **Unified, modular structure**: Separate folders and scripts for streaming and batch, with shared configuration.
- **Dynamic symbol management**: Always up-to-date with market listings.
- **Robust error handling**: Handles API limits, network issues, and file errors.
- **Scalable and portable**: Designed for deployment via Docker Compose, with all services orchestrated together.
- **Documentation and testing**: Each module includes clear README, example scripts, and (where relevant) tests.

---

## How to Use

1. **Start all services** (Kafka, Zookeeper, NiFi) with Docker Compose.
2. **Run the streaming producer** to ingest real-time data into Kafka.
3. **Drop batch files** (CSV/Parquet) into the monitored directory for NiFi to process.
4. **Monitor** the pipelines via logs, Kafka topics, and the NiFi web interface.

---

## What was implemented

- A complete, production-ready ingestion layer for both streaming and batch data.
- Dynamic, automated symbol management for financial data ingestion.
- Centralized error handling and logging for reliability.
- Integration with Kafka and NiFi for scalable, real-world data pipelines.
- Modular, extensible codebase ready for further development and integration.

---

For more details, see the documentation in each subfolder and the main project README.
