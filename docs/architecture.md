# FinanceLake Big Data Architecture

![Architecture Diagram](architecture-diagram.png Descriptions)

### 1. Data Sources

- **Real-Time Data Sources**  
  Financial data is collected in real time from APIs such as Yahoo Finance API and Alpha Vantage API.  
  These sources provide information like stock prices, news, alerts, and potentially security logs.

- **Batch Data Sources**  
  Batch data includes historical files, logs, or CSVs collected at intervals.

### 2. Data Ingestion

- **Kafka Data Ingestion**  
  Kafka is used for ingesting real-time data streams.  
  It manages multiple topics: stock prices, news, alerts, firewall logs, etc.  
  Kafka enables high-throughput, reliable, and scalable streaming data ingestion.

- **NiFi Data Ingestion**  
  Apache NiFi orchestrates batch data ingestion.  
  It facilitates the movement, transformation, and tracking of data from multiple sources.

### 3. Data Processing

- **Real-Time Processing**  
  Spark Streaming consumes messages from Kafka in real time.  
  **Role:** Read, process, and store data as soon as it arrives.  
  **Technology:** Apache Spark with Kafka integration.

- **Batch Processing**  
  Apache Spark processes batch data ingested via NiFi.  
  **Role:** Perform analytics or data cleaning on large historical datasets.

### 4. Storage Layer

- **Data Warehouse:** (e.g., Snowflake) for analytics and reporting.
- **Data Lake:** (e.g., S3) for storing raw data (JSON, CSV files).
- **Relational Database:** (MySQL, PostgreSQL) for user management and authentication.

### 5. APIs

- **REST APIs:**  
  Exposed via FastAPI (Python) or Spring Boot (Java) for client applications to access data.
- **GraphQL APIs:**  
  Exposed via Apollo Server for flexible and optimized queries.

### 6. Frontend

- **Web:**  
  Web applications built with React.js, Typescript, Javascript, or Dash for data visualization.
- **Mobile:**  
  Mobile applications built with React Native for smartphone access.

### 7. Analytics

- **Analytics Engines:**  
  Machine Learning frameworks such as TensorFlow and Scikit-Learn for predictive analysis.
- **Visualization:**  
  Tools like Tableau, PowerBI, or custom dashboards for exploring and visualizing financial indicators.

### 8. DevOps

- **Cloud:** AWS for deployment.
- **CI/CD:** GitHub Actions for automated testing and deployment.
- **Security:** TLS/SSL for secure data flows.

---

## Architecture Rationale

This architecture was chosen to efficiently handle large volumes of financial data, both in real time and batch mode.  
It is based on robust, scalable, and widely adopted open-source technologies.  
Separating each layer (ingestion, processing, storage, exposure, visualization) brings flexibility, maintainability, and allows for easy addition of new data sources or analytics modules in the future.  
Finally, the integration of advanced analytics and visualization tools meets the needs of users for analysis and data-driven decision making.


