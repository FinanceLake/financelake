# Data Storage Layer Design for FinanceLake

## 1. Introduction

This document outlines the design of a data storage layer for the FinanceLake project. The layer is designed to store cleaned and processed financial data in a structured format, optimized for analytical queries and efficient storage. The design follows the Delta Lake architecture with a medallion pattern (Bronze, Silver, Gold layers) to ensure data quality progression and analytical readiness. The data ingestion component is already implemented using Kafka and is available in the data-ingestion-kafka folder.

## Video Demonstration
<video src="resources/vid/FinanceLake Architecture Demo.mp4" width="640" height="360" controls>
  Your browser does not support the video tag.
</video>

## 2. Data Format Choice
## Architecture Diagram

### High-Level Architecture

![FinanceLake Architecture](resources/img/Finance_Lake_Architecture.PNG)

*Figure 1: High-level architecture of the FinanceLake data storage layer*

### Detailed Architecture Components

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            FinanceLake Data Lake                            │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                          Data Sources                                  │ │
│  │                 (APIs, Databases, Files, Streams)                      │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                      V                                      │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                          Data Ingestion                                │ │
│  │              (Apache Kafka, Apache NIFI, Airbyte, etc)                 │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                      V                                      │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                    Apache Spark Processing Engine                      │ │
│  │          - ETL Transformations     - Feature Engineering               │ │
│  │          - Data Quality Validation    - KPI Calculations               │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                      V                                      │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                       Delta Lake Storage Layer                         │ │
│  │  ┌───────────────┐        ┌──────────────┐          ┌─────────────┐    │ │
│  │  │ Bronze Layer  │        │ Silver Layer │          │ Gold Layer  │    │ │
│  │  │ - Raw Data    │        │ - Processed  │          │ - Analytics │    │ │
│  │  │ - Partitioned │        │ - Enriched   │          │ - Aggregated│    │ │
│  │  │ by date/region│        │ - Business   │          │ - Optimized │    │ │
│  │  └───────────────┘        │ Logic Applied│          │ for Queries │    │ │
│  │                           └──────────────┘          └─────────────┘    │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                      V                                      │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                          HDFS Storage Backend                          │ │
│  │  - Distributed File System                                             │ │
│  │  - Partitioned Storage (date=YYYY-MM-DD/region=XX/)                    │ │
│  │  - Replication for Fault Tolerance                                     │ │
│  │  - Hadoop Ecosystem Integration                                        │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                      V                                      │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                      Query & Analytics Layer                           │ │
│  │  - Apache Spark SQL                                                    │ │
│  │  - BI Tools (Tableau, Power BI)                                        │ │
│  │  - ML/AI Workloads                                                     │ │
│  │  - Real-time Dashboards                                                │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                      V                                      │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │              Deep Learning & Predictive Analytics Layer                │ │
│  │  - RNN/LSTM Models for Market Trend Prediction                         │ │
│  │  - Real-time Anomaly Detection on Silver Layer Data                    │ │
│  │  - Feature Engineering (Technical Indicators, Normalization)           │ │
│  │  - Distributed Model Training with Spark MLlib                         │ │
│  │  - Batch & Stream Inference Pipeline                                   │ │
│  │  - Prediction Storage in Gold Layer for Analytics                      │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```
### Data Flow Architecture

```
Data Sources → Data Ingestion → Apache Spark → Delta Lake Layers → HDFS Storage → Deep Learning Models → Analytics Tools

Detailed Flow:
1. Raw data ingestion via Kafka producers and Spark Structured Streaming (see data-ingestion-kafka folder)
2. Data cleansing and initial partitioning (Bronze)
3. Business logic application and enrichment (Silver)
4. Aggregation and optimization for analytics (Gold)
5. Query optimization with partitioning and Z-ordering
6. Deep Learning Analysis & Predictions:
   - RNN/LSTM models trained on Silver layer historical data
   - Real-time trend prediction and anomaly detection
   - Feature engineering with technical indicators and normalized metrics
   - Integration with Spark MLlib for distributed model inference
   - Predictions fed back to Gold layer for enhanced analytics
7. Backup and disaster recovery via HDFS features
```

### Chosen Format: Delta Lake with Medallion Architecture

We have selected **Delta Lake** as the primary data format, implemented using the **Medallion Architecture** (Bronze, Silver, Gold layers).

#### Medallion Architecture Layers:
- **Bronze Layer**: Raw, ingested data with minimal processing. Stores data as-is from sources.
- **Silver Layer**: Cleaned and transformed data with basic validations and enrichments.
- **Gold Layer**: Aggregated, business-ready data optimized for analytics and reporting.

#### Justification:
- **Performance de requête**: Delta Lake provides ACID transactions, schema enforcement, and optimized query performance through features like data skipping, Z-ordering, and automatic file compaction. Queries on Delta tables are significantly faster than traditional Parquet files due to metadata optimization and statistics collection.
- **Efficacité de stockage**: Delta Lake uses columnar storage (Parquet) with compression, reducing storage costs by 30-50% compared to traditional formats. Time travel capabilities allow efficient storage of historical versions without duplication.
- **Additional Benefits**: Schema evolution, unified batch and streaming, integration with Apache Spark, and support for machine learning workloads.

## 3. Storage Architecture

### Distributed File System: HDFS

We will use **HDFS** as the underlying distributed file system for the following reasons:
- Scalability: Virtually unlimited storage capacity by adding nodes
- Durability: Fault tolerance through data replication
- Cost-effectiveness: Open-source solution for on-premises deployments
- Integration: Native support with Delta Lake and Apache Spark
- Ecosystem: Full integration with Hadoop tools (MapReduce, Hive, etc.)

#### Integration Steps:
1. **HDFS Cluster Configuration**: Deploy a Hadoop cluster with NameNode and DataNodes. Use tools like Apache Ambari or Cloudera Manager for management.
2. **Connection with Kafka**: Use Kafka Connect with the HDFS Sink connector (file `hdfs-sink.json`) to ingest data in real-time from Kafka topics to HDFS. Configure parameters such as file format (Parquet, Avro), partitioning, and file rotation.
3. **Processing with Spark**: Configure Apache Spark to read and write data on HDFS. Use HDFS paths (hdfs://namenode:port/path) in Spark jobs for storing Bronze, Silver, and Gold layers.
4. **Partitioning Schema**: Apply partitioning similar to S3, for example `/bronze/stock_data/date=YYYY-MM-DD/region=XX/`, to optimize queries.
5. **Security and Authentication**: Implement Kerberos for authentication and HDFS ACLs for access control.

#### Example Kafka Connect HDFS Sink Configuration:
The `hdfs-sink.json` file defines parameters to connect Kafka to HDFS, including storage format, source topics, and destination paths.

This setup allows maintaining the same layered architecture (Bronze, Silver, Gold) using HDFS as the storage backend.

## 4. Data Organization

### Partitioning Strategy

Data will be partitioned based on frequently queried columns to optimize query performance:

- **Primary Partitioning**: `date` (partitioned by year/month/day for time-series analysis)
- **Secondary Partitioning**: `region` (geographic partitioning for regional analytics)
- **Additional Partitioning**: `data_type` or `asset_class` for financial data categorization

Example partition structure:
```
hdfs://namenode:9000/financelake-data/
├── bronze/
│   ├── stock_data/
│   │   ├── date=2024-01-01/
│   │   │   ├── region=US/
│   │   │   └── region=EU/
│   │   └── date=2024-01-02/
│   └── bond_data/
├── silver/
└── gold/
```

## 5. Performance Optimization

### Techniques Employed:

1. **Partitioning**: As described above, reduces data scanning for queries with date/region filters
2. **Z-ordering**: Optimizes data layout for multi-dimensional queries by co-locating related data
3. **Caching**: Spark caching for frequently accessed datasets
4. **Compaction**: Automatic file compaction to optimize storage and query performance
5. **Bloom Filters**: For efficient point lookups on high-cardinality columns

### Query Optimization:
- Use Delta Lake's OPTIMIZE command for file compaction
- Implement ZORDER BY on commonly filtered columns (date, symbol, region)
- Leverage Spark's Catalyst optimizer with Delta statistics

## 6. Integration with Apache Spark

The storage layer integrates seamlessly with Apache Spark through the Delta Lake library:

- **Batch Processing**: Spark DataFrames for ETL operations
- **Streaming**: Structured Streaming for real-time data ingestion
- **SQL Support**: Spark SQL for analytical queries
- **ML Integration**: Direct integration with Spark MLlib for model training on stored data

## 7. Predictive Analytics with RNN for Market Trend Prediction

To leverage the cleaned and enriched data in the Silver layer for predictive analytics, we propose implementing a Recurrent Neural Network (RNN) architecture, specifically using Long Short-Term Memory (LSTM) networks, to predict market trends for financial symbols.

### Data Preparation from Silver Layer
- **Input Data**: Use Silver layer data containing historical price, volume, and enriched features for each symbol.
- **Feature Engineering**: 
  - Normalize price and volume data (e.g., using Min-Max scaling).
  - Create technical indicators (e.g., moving averages, RSI, MACD) as additional features.
  - Generate sequences of fixed length (e.g., 60 time steps) for each prediction.
- **Target Variable**: Define trend labels (e.g., 1 for upward trend, 0 for downward trend, 2 for neutral) based on future price movements (e.g., price change over the next 5 days).

### Proposed RNN Architecture
```
Input Layer (Sequence of 60 time steps, features: price, volume, indicators)
    ↓
LSTM Layer 1 (128 units, return sequences=True)
    ↓
Dropout (0.2)
    ↓
LSTM Layer 2 (64 units, return sequences=False)
    ↓
Dropout (0.2)
    ↓
Dense Layer (32 units, ReLU activation)
    ↓
Output Layer (3 units, Softmax activation for multi-class classification)
```

#### Architecture Details:
- **LSTM Layers**: Two stacked LSTM layers to capture temporal dependencies in time-series data. The first layer returns sequences for deeper learning, the second aggregates to a single output.
- **Dropout**: Applied after each LSTM layer to prevent overfitting.
- **Dense Layer**: Fully connected layer for feature combination.
- **Output**: Softmax for predicting trend classes (up, down, neutral).
- **Loss Function**: Categorical Cross-Entropy.
- **Optimizer**: Adam with learning rate 0.001.
- **Metrics**: Accuracy, Precision, Recall, F1-Score.

### Training and Deployment
- **Training Data**: Split Silver layer data into train/validation/test sets (e.g., 70/20/10).
- **Batch Size**: 64, Epochs: 100 with early stopping.
- **Framework**: TensorFlow/Keras or PyTorch.
- **Deployment**: Train the model on historical data, deploy as a service to predict trends for new data ingested into Silver layer.
- **Integration**: Use Spark MLlib or external ML pipelines to preprocess data and feed into the RNN model.

### Benefits
- **Accuracy**: RNNs excel at capturing patterns in sequential data, improving trend prediction accuracy.
- **Real-time Prediction**: Once trained, the model can predict trends for incoming data in near real-time.
- **Scalability**: Deploy on distributed systems for handling large-scale financial data.

This RNN architecture provides a foundation for advanced analytics on the FinanceLake data lake, enabling proactive decision-making based on predicted market trends.

## 8. Service Management Scripts

The project includes two critical bash scripts for managing the complete FinanceLake pipeline: `start_services.sh` and `stop_services.sh`. These scripts automate the deployment and lifecycle management of all system components.

### start_services.sh - Service Initialization

**Purpose**: Orchestrates the startup of all required services in the correct sequence, ensuring proper initialization and dependency management.

**Key Functions**:

1. **Environment Configuration**
   - Sets environment variables for HADOOP_HOME, SPARK_HOME, KAFKA_HOME, and JAVA_HOME
   - Configures Kafka bootstrap servers, topics, and web application URLs
   - Initializes Python paths for Spark and PySpark compatibility

2. **Pre-flight Checks**
   - Verifies installation directories for Hadoop, Spark, and Kafka
   - Stops any pre-existing services to ensure clean startup
   - Creates log directory structure for centralized logging

3. **Service Startup Sequence**
   - **Zookeeper**: Starts first as a dependency for Kafka coordination
   - **Kafka**: Initializes after Zookeeper is ready
   - **Topic Creation**: Automatically creates/recreates the Kafka topic with proper partitioning
   - **HDFS/YARN**: Starts Hadoop distributed file system and resource manager if available
   - **Web Application**: Flask-based web interface for data visualization
   - **Kafka Producer**: Initiates data ingestion from Yahoo Finance via Kafka
   - **Spark Consumer**: Launches structured streaming consumer with Kafka integration

4. **Health Checks**
   - Implements port availability checks (TCP/nc) before proceeding
   - Waits for Kafka broker to become available with configurable timeout
   - Verifies successful topic creation before continuing

5. **Logging & Monitoring**
   - Captures all service outputs to individual log files in `./log/` directory
   - Stores process IDs (PIDs) for later management and cleanup
   - Provides real-time startup progress indicators

### stop_services.sh - Service Termination

**Purpose**: Gracefully shuts down all running services in reverse startup order, ensuring proper cleanup and data integrity.

**Key Functions**:

1. **Process Cleanup**
   - Terminates Spark consumer via stored PID files
   - Kills all Python processes (app.py, kafka_producer.py)
   - Stops Kafka broker and Zookeeper services
   - Removes PID files for clean state management

2. **Graceful Shutdown**
   - Uses process ID files for precise termination (avoiding wrong process termination)
   - Implements error handling with `|| true` to continue cleanup even if some processes are already stopped
   - Ensures no zombie processes remain in the system

3. **Clean State**
   - Removes all PID tracking files
   - Allows for clean restart without resource conflicts

### Integration with the Pipeline

These scripts enable:
- **Reproducible Deployments**: Consistent startup order and configuration across environments
- **Development Workflow**: Quick iteration between testing and restarting with `./stop_services.sh && ./start_services.sh`
- **Production Readiness**: Automated health checks and monitoring capabilities
- **Fault Tolerance**: Automatic recovery by restarting failed services through scheduled cron jobs

### Log Files Reference

| Service | Log File | Purpose |
|---------|----------|---------|
| Web Application | `log/web_app.log` | Flask web server logs |
| Kafka Producer | `log/kafka_producer.log` | Data ingestion and producer metrics |
| Spark Consumer | `log/structured_spark_consumer_v6.log` | Streaming ETL and delta table writes |
| Kafka Broker | `log/kafka.log` | Message broker operations |
| Zookeeper | `log/zookeeper.log` | Coordination service logs |
| HDFS | `log/hdfs.log` | Distributed file system logs |
| YARN | `log/yarn.log` | Resource manager logs |

### Backup Strategy:
1. **Point-in-Time Snapshots**: Daily snapshots of Gold layer data
2. **Cross-Region Replication**: S3 cross-region replication for disaster recovery
3. **Versioning**: Delta Lake t     ime travel for historical data access
4. **Incremental Backups**: For Silver layer using Delta's change data feed

### Recovery Strategy:
1. **Automated Failover**: Multi-region deployment with automatic failover
2. **Data Restoration**: Restore from snapshots within minutes
3. **Point-in-Time Recovery**: Use Delta time travel for granular recovery
4. **RTO/RPO Targets**: Recovery Time Objective < 1 hour, Recovery Point Objective < 15 minutes

## 8. Testing Scalability and Performance

### Performance Metrics:
- **Query Latency**: Target < 5 seconds for 95th percentile on Gold layer queries
- **Storage Cost**: Optimize for < $0.05/GB/month
- **Throughput**: Support 10,000+ concurrent queries

### Scalability Testing:
1. **Load Testing**: Simulate increasing data volumes (1TB to 100TB)
2. **Concurrency Testing**: Test with 100-1000 concurrent users
3. **Query Performance**: Benchmark complex analytical queries
4. **Storage Scaling**: Test auto-scaling of S3 storage

### Monitoring:
- Implement CloudWatch metrics for S3 and Spark performance
- Delta Lake metrics for table operations and query performance
- Automated alerts for performance degradation

## 9. Implementation Roadmap

1. **Phase 1**: Set up Delta Lake infrastructure and Bronze layer
2. **Phase 2**: Implement Silver layer transformations
3. **Phase 3**: Build Gold layer aggregations
4. **Phase 4**: Performance optimization and testing
5. **Phase 5**: Backup/recovery implementation and monitoring
6. **Phase 6**: Deep Learning & Predictive Analytics implementation
   - Deploy RNN/LSTM models for market trend prediction
   - Implement feature engineering pipeline from Silver layer data
   - Set up distributed model training with Spark MLlib
   - Configure real-time inference pipeline for streaming data
   - Integrate predictions into Gold layer for enriched analytics
   - Monitor model performance and implement retraining schedules

## 10. Conclusion

This design provides a robust, scalable, and performant data storage layer using Delta Lake and Medallion architecture. The chosen technologies ensure efficient storage, fast analytical queries, and reliable data management for the FinanceLake project.