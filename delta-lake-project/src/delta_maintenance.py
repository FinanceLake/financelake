import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# --- CONFIGURATION ---
base_path = os.getcwd()
delta_path_bronze = "file://" + os.path.join(base_path, "finance_lake/bronze/raw_events")

# Initialize Spark with Delta Support
builder = SparkSession.builder \
    .appName("DeltaMaintenance") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") # ALLOW 0 HOURS FOR DEMO ONLY

spark = configure_spark_with_delta_pip(builder).getOrCreate()

print(f"Connecting to Delta Table at: {delta_path_bronze}")

if DeltaTable.isDeltaTable(spark, delta_path_bronze):
    deltaTable = DeltaTable.forPath(spark, delta_path_bronze)

    print("--- Running VACUUM (Removing old files) ---")
    # 'RETAIN 0 HOURS' is dangerous in production (default is 7 days), 
    # but we use it here to force immediate cleanup for the demo purpose.
    deltaTable.vacuum(0) 

    print(">> VACUUM COMPLETE: Old Parquet files removed. <<")
else:
    print("XX Error: Path is not a Delta Table.")