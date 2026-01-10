import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.spark_config import create_spark_session
from pyspark.sql.functions import from_json, col, current_timestamp, lit
from pyspark.sql.types import *
from pyspark.sql import Row
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

BRONZE_PATH = "hdfs://localhost:9000/financelake/bronze"
BRONZE_CHECKPOINT = "hdfs://localhost:9000/financelake/checkpoints/bronze"
ERROR_LOG_PATH = "hdfs://localhost:9000/financelake/error_logs"

schema = StructType([
    StructField("Open", DoubleType()),
    StructField("High", DoubleType()),
    StructField("Low", DoubleType()),
    StructField("Close", DoubleType()),
    StructField("Volume", DoubleType()),
    StructField("ticker", StringType()),
    StructField("transaction_id", StringType()),
    StructField("event_time", StringType()) 
])

spark = create_spark_session()

error_schema = StructType([
    StructField("batch_id", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

empty_error_df = spark.createDataFrame([], schema=error_schema)

try:
    DeltaTable.forPath(spark, ERROR_LOG_PATH)
    print("Error logs table already exists.")
except AnalysisException:
    print("Creating empty error logs table...")
    empty_error_df.write.format("delta").mode("overwrite").save(ERROR_LOG_PATH)


def log_error(batch_id, error_message):
    error_row = spark.createDataFrame([
        Row(batch_id=batch_id, error_message=error_message, timestamp=datetime.now())
    ])
    error_row.write.format("delta").mode("append").save(ERROR_LOG_PATH)


def process_batch(batch_df, batch_id):
    try:
        transformed_df = (
            batch_df.selectExpr("CAST(value AS STRING) as json_str")
            .select(from_json(col("json_str"), schema).alias("data"))
            .select("data.*")
            .withColumn("event_time", (col("event_time").cast("long") / 1000).cast("timestamp"))
            .withColumn("ingest_time", current_timestamp())
            .withColumn("batch_id", lit(batch_id))
        )
        
        transformed_df.write.format("delta").mode("append").save(BRONZE_PATH)
        
        print(f"Batch {batch_id} processed successfully.")
    except Exception as e:
        print(f"Error processing batch {batch_id}: {e}")
        log_error(batch_id, str(e))


raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "finance")
    .load()
)

query = raw_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", BRONZE_CHECKPOINT) \
    .start()

query.awaitTermination()
