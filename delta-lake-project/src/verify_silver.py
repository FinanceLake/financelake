import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

# --- CONFIGURATION ---

base_path = os.getcwd()

# Source: Silver Delta Table (The table created by Person 2)
delta_path_silver = "file://" + os.path.join(
    base_path,
    "finance_lake/silver/cleaned_events"
)

# --- SPARK INITIALIZATION WITH DELTA SUPPORT ---

builder = (
    SparkSession.builder
    .appName("VerifySilver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"\n--- Reading from Silver Delta Table: {delta_path_silver} ---")

# --- READ SILVER TABLE IN BATCH MODE ---

try:
    df_silver = spark.read.format("delta").load(delta_path_silver)
except Exception as e:
    print(
        "\n❌ ERROR: Could not read Silver Table. "
        "Ensure silver_pipeline.py has run successfully at least twice. "
        f"Error: {e}"
    )
    spark.stop()
    exit()

print(f"\n✅ Total Cleaned Records in Silver Layer: {df_silver.count()}")
print("-------------------------------------------------------")

# --- DEDUPLICATION CHECK (PROOF OF MERGE) ---

print("\n--- Deduplication Check: Finding event_id with count > 1 ---")

df_duplicates = (
    df_silver
    .groupBy("event_id")
    .count()
    .filter("count > 1")
)

if df_duplicates.count() > 0:
    print("❌ FAILURE: Duplicates found in the Silver Layer!")
    df_duplicates.show()
else:
    print("✅ SUCCESS: No duplicates found. MERGE operation works correctly.")

# --- SAMPLE DATA VIEW ---

print("\n--- Sample Cleaned Data (Primary Key + Audit Columns) ---")

df_silver.select(
    "event_id",
    "symbol",
    "price",
    "timestamp",
    "silver_processing_time"
).show(5, truncate=True)

# --- STOP SPARK ---

spark.stop()

