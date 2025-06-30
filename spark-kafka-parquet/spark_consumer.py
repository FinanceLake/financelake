import os
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col

# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

try:
    # --- Load configuration ---
    config_path = os.path.join(os.path.dirname(__file__), 'config', 'spark_config.json')
    with open(config_path) as config_file:
        config = json.load(config_file)

    # --- Ensure output directories exist ---
    os.makedirs(config["parquet_path"], exist_ok=True)
    os.makedirs(config["checkpoint_path"], exist_ok=True)

    # --- Initialize SparkSession ---
    spark = SparkSession.builder \
        .appName("KafkaToParquetConsumer") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # --- Define schema for Kafka message ---
    schema = StructType() \
        .add("symbol", StringType()) \
        .add("price", DoubleType()) \
        .add("timestamp", TimestampType())

    # --- Read streaming data from Kafka ---
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config["bootstrap_servers"]) \
        .option("subscribe", config["topic"]) \
        .option("startingOffsets", "latest") \
        .load()

    # --- Convert Kafka value bytes to JSON ---
    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*")

    # --- Write stream to Parquet ---
    query = json_df.writeStream \
        .format("parquet") \
        .option("path", config["parquet_path"]) \
        .option("checkpointLocation", config["checkpoint_path"]) \
        .trigger(processingTime=config["trigger_interval"]) \
        .start()

    logging.info("Spark streaming job started. Awaiting termination...")
    query.awaitTermination()

except Exception as e:
    logging.error(f"An error occurred: {e}", exc_info=True)