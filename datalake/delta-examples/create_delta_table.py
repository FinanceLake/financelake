from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

spark = SparkSession.builder \
    .appName("FinanceLake-DeltaLake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
# Example: Create a Delta table from existing Parquet tables (from spark-kafka-parquet)
data_path = "/opt/spark-data/parquet/stocks/"
df = spark.read.parquet(data_path)

delta_path = "/opt/spark-data/delta/stocks"
df.write.format("delta").mode("overwrite").save(delta_path)

# Create a catalog table
spark.sql(f"CREATE TABLE stocks_delta USING DELTA LOCATION '{delta_path}'")

# Exemple Spark SQL
spark.sql("""
    SELECT symbol, AVG(close) as avg_price, MAX(volume) as max_volume
    FROM stocks_delta
    WHERE date >= '2025-01-01'
    GROUP BY symbol
    ORDER BY avg_price DESC
""").show(10)
