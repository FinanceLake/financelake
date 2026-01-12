from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, stddev, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from delta import configure_spark_with_delta_pip
import os

# --- CONFIGURATION ---
base_path = os.getcwd()
# Define the path for the Bronze Delta Table (Raw, Audited Data)
delta_path_bronze = "file://" + os.path.join(base_path, "finance_lake/bronze/raw_events")

# !!! FIX: ADD "file://" PREFIX HERE !!!
checkpoint_path = "file://" + os.path.join(base_path, "checkpoints/bronze_raw")

print(f"!!! BRONZE DELTA TABLE WILL BE SAVED TO: {delta_path_bronze} !!!")
print(f"!!! CHECKPOINT LOCATION: {checkpoint_path} !!!")

# Initialize Spark with Delta Support
builder = SparkSession.builder \
    .appName("DeltaLakeBronzeStream") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "2")

# Get or Create the configured Spark Session (using utility to include packages)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema (Raw Input)
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("timestamp", StringType(), True) 
])

# Read Stream
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse Raw Stream
parsed_stream = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")
parsed_stream = parsed_stream.withColumn("timestamp", col("timestamp").cast(TimestampType()))

# --- BRONZE LAYER PROCESSING FUNCTION ---
def process_raw_batch(df, epoch_id):
    count = df.count()
    if count == 0:
        return

    print(f"\n--- Bronze Stream Batch {epoch_id}: Processing {count} raw rows ---")

    # 1. Add Audit Columns (Crucial for Bronze Layer Archiving)
    df_audited = df.withColumn("ingestion_time", current_timestamp()) \
                   .withColumn("source_system", lit("socket_stream"))
    
    # Show sample data before writing
    df_audited.show(5, truncate=False)

    # 2. Write to Delta Lake (Transactional Append)
    try:
        print(f"Attempting to write to: {delta_path_bronze} ...")
        df_audited.write \
          .format("delta") \
          .mode("append") \
          .save(delta_path_bronze)
        print(">> DELTA WRITE SUCCESS (STREAM)! <<")
    except Exception as e:
        print(f"XX DELTA WRITE FAILED: {e}")

# --- START THE STREAM QUERY (Raw Data to Bronze Delta Table) ---
query = parsed_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(process_raw_batch) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

query.awaitTermination()