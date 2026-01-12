import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import current_timestamp, lit, col
from delta import configure_spark_with_delta_pip

# --- CONFIGURATION ---
base_path = os.getcwd()
# Destination: The same Bronze Delta Table as the streaming job
delta_path_bronze = "file://" + os.path.join(base_path, "finance_lake/bronze/raw_events")
# Source: The local historical data file
batch_source_path = "file://" + os.path.join(base_path, "historical_data_delta.json")

print(f"!!! BATCH SOURCE: {batch_source_path} !!!")
print(f"!!! DESTINATION: {delta_path_bronze} !!!")

# Define the Schema (Same as streaming input)
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

# Initialize Spark with Delta Support (MUST match the streaming config)
builder = SparkSession.builder \
    .appName("DeltaLakeBatchLoader") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "2")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# --- 1. READ BATCH DATA ---
print("\n--- Starting Batch Read ---")
try:
    # Read the JSON file (Batch Mode)
    df_batch = spark.read.schema(schema).json(batch_source_path)
    
    # Cast timestamp column
    df_batch = df_batch.withColumn("timestamp", col("timestamp").cast(TimestampType()))

except Exception as e:
    print(f"❌ Error reading batch source file: {e}")
    spark.stop()
    exit()

# --- 2. ADD AUDIT COLUMNS ---
# Set the source_system to 'historical_batch_load'
df_audited = df_batch.withColumn("ingestion_time", current_timestamp()) \
                   .withColumn("source_system", lit("historical_batch_load"))

print(f"Batch loaded. Records to write: {df_audited.count()}")
df_audited.show(5, truncate=False)

# --- 3. WRITE TO DELTA BRONZE SINK (Unified Append) ---
print(f"\nAttempting to write records to the shared Bronze Delta Table...")
try:
    df_audited.write \
        .format("delta") \
        .mode("append") \
        .save(delta_path_bronze)
    print(">> BATCH WRITE SUCCESS (DELTA)! <<")
except Exception as e:
    print(f"XX DELTA BATCH WRITE FAILED: {e}")

spark.stop()