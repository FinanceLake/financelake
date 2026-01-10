from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

def inspect_bronze():
    spark = SparkSession.builder \
        .appName("BronzeInspector") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    bronze_path = "/app/data/lake/bronze"
    
    print(f"Reading Delta table from {bronze_path}...")
    try:
        df = spark.read.format("delta").load(bronze_path)
        
        print("\n--- Total Record Count ---")
        print(df.count())

        print("\n--- Sample Data (Top 5) ---")
        df.show(5, truncate=False)

        print("\n--- Duplicate Check (Group by Symbol, Timestamp) ---")
        duplicates = df.groupBy("symbol", "timestamp").count().filter("count > 1").orderBy(desc("count"))
        
        dup_count = duplicates.count()
        print(f"Found {dup_count} unique symbol-timestamp pairs with duplicates.")
        
        if dup_count > 0:
            print("Top 10 Duplicates:")
            duplicates.show(10, truncate=False)
        else:
            print("No duplicates found based on Symbol + Timestamp.")

    except Exception as e:
        print(f"Error reading Bronze table: {e}")

if __name__ == "__main__":
    inspect_bronze()
