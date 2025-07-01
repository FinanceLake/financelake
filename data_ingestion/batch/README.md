# Batch Ingestion – FinanceLake

This module implements the **batch data ingestion pipeline** for FinanceLake, orchestrated by Apache NiFi.

## What does the code do?

- **Automates the ingestion of batch files** (CSV, Parquet) using Apache NiFi.
- **Monitors a directory** for new files, processes them, and sends each record to a Kafka topic.
- **Handles all batch logic in NiFi**: No redundant Python scripts; all parsing, transformation, and routing are managed visually in NiFi.
- **Integrates with Docker**: Volumes are configured so NiFi can access files on the host system.
- **Extensible and maintainable**: Easily add new file formats or processing steps in NiFi.

## Key Features

- **Automated file monitoring**: New files are detected and processed without manual intervention.
- **Centralized orchestration**: All batch logic is managed in NiFi for easy monitoring and modification.
- **Kafka integration**: Each record is sent to a Kafka topic for downstream processing.
- **Docker-ready**: Works seamlessly with Docker Compose setups.

## How to use

1. **Start NiFi, Kafka, and Zookeeper** (e.g., with Docker Compose).
2. **Access the NiFi web interface** at [http://localhost:8080/nifi](http://localhost:8080/nifi).
3. **Import and configure the NiFi flow** (see `nifi_flow.json`).
4. **Drop batch files** (CSV/Parquet) into the monitored directory (e.g., `data_ingestion/batch/data/`).
5. **Monitor**: Use the NiFi UI and Kafka tools to track progress and troubleshoot.

## Files

- `nifi_flow.json` : Example NiFi flow for batch ingestion.
- `README.md` : This documentation.

---

For more details, see the global README in `data_ingestion/`.
