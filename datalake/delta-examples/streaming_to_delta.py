from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("FinanceLake-StreamingToDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Read Kafka streaming
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stock-ticks") \
    .load()

# Parser JSON
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

# Write in Delta Lake with checkpoint
query = parsed_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/opt/checkpoints/stock-stream") \
    .partitionBy("date") \
    .table("stocks_live_delta")

query.awaitTermination()
