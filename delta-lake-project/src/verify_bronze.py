import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# --- CONFIGURATION ---
base_path = os.getcwd()
delta_path_bronze = "file://" + os.path.join(base_path, "finance_lake/bronze/raw_events")

# Initialize Spark
builder = SparkSession.builder \
    .appName("VerifyBronze") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

print(f"Reading from: {delta_path_bronze}")

# Read the Bronze Table
df = spark.read.format("delta").load(delta_path_bronze)

print(f"Total Record Count: {df.count()}")

print("\n--- Breakdown by Source System ---")
df.groupBy("source_system").count().show()

print("\n--- Sample Data ---")
df.show(5)