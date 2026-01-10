"""
Central Configuration for Spark Streaming
BEST PRACTICE: Separate configuration from business logic
"""

class SparkConfig:
    """Optimized Spark Configuration for Financial Streaming (Low Resource Mode)"""
    
    # Kafka Configuration
    KAFKA_BROKERS = "redpanda:9094" # Ensure this matches docker-compose ports
    KAFKA_TOPIC = "stock_prices"
    KAFKA_STARTING_OFFSETS = "earliest" 
    
    # Spark Configuration
    APP_NAME = "StockInsightStreaming"
    LOG_LEVEL = "WARN"
    
    # Performance Tuning
    # SENIOR FIX: Drastically throttle input to prevent OOM
    MAX_OFFSETS_PER_TRIGGER = 200 
    PROCESSING_TIME = "0 seconds" 
    
    # Storage paths (MinIO/S3)
    BASE_PATH = "s3a://finance-lake"
    BRONZE_PATH = f"{BASE_PATH}/lake/bronze"
    SILVER_PATH = f"{BASE_PATH}/lake/silver"
    GOLD_PATH = f"{BASE_PATH}/lake/gold"
    CHECKPOINT_ROOT = f"{BASE_PATH}/checkpoints"
    
    # Windowing Configuration
    WINDOW_DURATION = "10 seconds"
    SLIDE_DURATION = "5 seconds" 
    WATERMARK_DELAY = "5 seconds"
    
    # Spark SQL Optimization
    # SENIOR FIX: Force single partition to save RAM
    SHUFFLE_PARTITIONS = 1
    # SENIOR FIX: Disable Adaptive Query Execution (it increases partitions automatically)
    ADAPTIVE_ENABLED = False
    
    # Delta Lake Configuration
    DELTA_MERGE_SCHEMA = True
    DELTA_AUTO_OPTIMIZE = True
    
    @staticmethod
    def get_spark_configs():
        """Returns a dict of Spark configurations"""
        configs = {
            "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,io.delta:delta-core_2.12:2.4.0",
            
            # --- DELTA ---
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.databricks.delta.schema.autoMerge.enabled": str(SparkConfig.DELTA_MERGE_SCHEMA).lower(),
            
            # --- STREAMING ---
            "spark.sql.streaming.minBatchesToRetain": "5",
            "spark.sql.streaming.schemaInference": "false",
            "spark.streaming.stopGracefullyOnShutdown": "true",
            "spark.sql.streaming.kafka.consumer.poll.ms": "512",
            
            # --- CRITICAL STABILITY SETTINGS ---
            "spark.sql.adaptive.enabled": "false",  # Must be false for small containers
            "spark.sql.adaptive.coalescePartitions.enabled": "false",
            "spark.sql.shuffle.partitions": str(SparkConfig.SHUFFLE_PARTITIONS),
            "spark.default.parallelism": str(SparkConfig.SHUFFLE_PARTITIONS),
            "spark.sql.streaming.statefulOperator.checkCorrectness.enabled": "false",

            # --- MEMORY SAFETY ---
            "spark.memory.fraction": "0.5",        
            "spark.memory.storageFraction": "0.1", 

            # --- S3 / MINIO CONFIG ---
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.committer.name": "directory", 
            "spark.sql.sources.commitProtocolClass": "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol",

            # --- OOM FIX: BUFFERING ---
            "spark.hadoop.fs.s3a.fast.upload": "true",
            "spark.hadoop.fs.s3a.fast.upload.buffer": "disk", 
            "spark.hadoop.fs.s3a.buffer.dir": "/tmp/spark-s3-buffer",
            "spark.hadoop.fs.s3a.multipart.size": "5M",
            "spark.hadoop.fs.s3a.block.size": "10M"
        }

        configs["spark.sql.streaming.checkpointLocation.root"] = SparkConfig.CHECKPOINT_ROOT
        return configs