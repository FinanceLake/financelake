# Financial Data Lakehouse with Delta Lake

## Demo Video
https://drive.google.com/file/d/1JjZoMtAA9dnUGkplsAyeBrY1I7kV40wJ/view?usp=sharing

---

## Overview
In this project, we design and implement a robust Financial Data Lakehouse based on Delta Lake and Apache Spark, following the Medallion Architecture (Bronze, Silver, Gold).

Our goal is to efficiently manage high-frequency financial stock data, combining both historical batch data and real-time streaming data into a clean, reliable, and analytics-ready system.

This architecture allows us to ensure data quality, scalability, auditability, and ACID guarantees, making it suitable for advanced analytics and predictive modeling.

---

## Project Structure

```
financelake/ ← root of the repository
├── delta-lake-project/ ← main Delta Lake project
│   ├── src/ ← Spark scripts and pipelines
│   ├── data/ ← sample input data (optional)
│   └── README.md ← project-specific documentation
│
├── reports/ ← project documentation
│   └── delta_lake_report.pdf
```

---

## Architecture Overview
Our system is built using the Medallion Architecture, which organizes data into progressive layers of quality and refinement.

All data is stored locally on the machine, ensuring full control over the storage lifecycle.

---

## Bronze Layer – Raw Ingestion
The Bronze layer is the entry point for all data, responsible for ingesting raw financial events from multiple sources.

### Data Sources
- **Historical Batch Data**  
  Large JSON files containing historical stock prices, ingested using `batch_loader_delta.py`.

- **Real-Time Streaming Data**  
  Live stock price updates processed continuously using `consumer_sql.py`.

### Reliability Features
- **ACID Transactions**  
  Delta Lake guarantees atomic and consistent writes, acting as a security guard to prevent partial or corrupted data.

- **Maintenance Operations**  
  A VACUUM process (`delta_maintenance.py`) removes obsolete files while preserving Time Travel capabilities.

---

## Silver Layer – Refinement & Inference
In the Silver layer, we clean, standardize, and enrich the raw financial data using `silver_pipeline.py`.

### Key Transformations
- **Data Cleaning**
  - Generation of a unique `event_id` using a cryptographic SHA-256 hash
  - Filtering invalid records (e.g., non-positive prices)

- **Deduplication**
  Delta Lake MERGE operations ensure exactly-once processing by updating matched records and inserting new ones.

- **Feature Engineering**
  - Computation of lag-based features such as log returns
  - Rolling sequences to capture temporal market dynamics

### AI Integration
- A GRU (Gated Recurrent Unit) deep learning model is integrated directly into the streaming pipeline.
- The model produces predictive signals (`gru_prediction`) in near real-time.

---

## Gold Layer – Consumption & Analytics
The Gold layer delivers business-aligned, analytics-ready datasets designed for decision-making and visualization.

### Key Outputs
- **Interactive Dashboard**  
  A Streamlit application consumes the Gold Delta Table to display insights.

- **Key Metrics**
  - Model accuracy (e.g., 77.78%)
  - Visual indicators highlighting predicted price jumps

- **Audit & Transparency**
  A comparison table aligns actual outcomes (labels) with model predictions for performance evaluation.

---

## Operational Reliability & Auditability
- **Data Quality Proof**  
  The script `verify_silver.py` confirms that deduplication logic produces zero duplicate keys in the Silver layer.

- **Time Travel Auditing**  
  Using Delta Lake’s `versionAsOf`, we can query previous versions of the Gold table, enabling full traceability of features used at any point in time.

---

## Key Technologies
- **Storage**: Delta Lake  
- **Processing Engine**: Apache Spark (Batch & Structured Streaming)  
- **Deep Learning**: GRU-based Sequential Modeling  
- **Visualization**: Streamlit  
- **Language**: Python
