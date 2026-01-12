import os
import time
import traceback
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, avg, sum as sum_, count, when, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# ====================================================================
# PATHS
# ====================================================================
BRONZE_PATH = "/app/storage/bronze_stock"
SILVER_PATH = "/app/storage/silver_stock"
GOLD_PATH = "/app/storage/gold_stock"

CKPT_BRONZE = "/app/storage/ckpt/bronze"
CKPT_SILVER = "/app/storage/ckpt/silver"
CKPT_GOLD = "/app/storage/ckpt/gold"


def cleanup_corrupted_delta():
    """Remove corrupted Delta tables and checkpoints"""
    print("Cleaning up corrupted Delta tables...")
    
    paths_to_clean = [
        BRONZE_PATH,
        SILVER_PATH,
        GOLD_PATH,
        CKPT_BRONZE,
        CKPT_SILVER,
        CKPT_GOLD
    ]
    
    for path in paths_to_clean:
        if os.path.exists(path):
            try:
                shutil.rmtree(path)
                print(f"Removed: {path}")
            except Exception as e:
                print(f"Error removing {path}: {e}")
    
    # Recreate directories
    for p in [BRONZE_PATH, SILVER_PATH, GOLD_PATH, CKPT_BRONZE, CKPT_SILVER, CKPT_GOLD]:
        os.makedirs(p, exist_ok=True)
    
    print("Cleanup complete!")


# ====================================================================
# SPARK SESSION
# ====================================================================
spark = (
    SparkSession.builder
        .appName("StockPipeline")
        .master("local[*]")
        .config("spark.jars.packages",
                "io.delta:delta-core_2.12:2.4.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.apache.kafka:kafka-clients:3.5.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ====================================================================
# SCHEMA - Use proper timestamp type
# ====================================================================
schema = StructType([
    StructField("timestamp", StringType()),  # We'll convert this to timestamp later
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", IntegerType()),
    StructField("rsi", DoubleType()),
    StructField("macd", DoubleType()),
    StructField("volatility", DoubleType()),
    StructField("price_change", DoubleType()),
    StructField("price_direction", IntegerType()),
    StructField("volume_change", IntegerType())
])

batch_counter = 0


# ====================================================================
# SILVER BATCH PROCESSOR
# ====================================================================
def process_silver_batch(df, batch_id):
    global batch_counter
    batch_counter += 1

    print(f"\n=== SILVER BATCH {batch_counter} (ID: {batch_id}) ===")

    df.persist()
    n = df.count()
    print(f"Batch size: {n}")

    if n == 0:
        print("Empty batch, skipping...")
        df.unpersist()
        return

    # ----------------------------------------------------------
    # 1) SAVE SILVER DATA - Keep timestamp as TimestampType
    # ----------------------------------------------------------
    try:
        # The timestamp is already converted to TimestampType in the stream
        df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(SILVER_PATH)

        print("✓ Silver saved")

    except Exception as e:
        print(f"❌ Silver Save Error: {e}")
        traceback.print_exc()

    # ----------------------------------------------------------
    # 2) TRAIN ML EVERY 5 BATCHES
    # ----------------------------------------------------------
    if batch_counter % 5 == 0 and n > 100:
        try:
            print("Training ML model...")

            features = ["rsi", "macd", "volatility", "price_change", "volume_change"]
            assembler = VectorAssembler(inputCols=features, outputCol="features", handleInvalid="skip")

            ml_df = assembler.transform(df).select("features", "price_direction")
            ml_df = ml_df.filter(col("features").isNotNull())

            if ml_df.count() < 50:
                print("Not enough data for ML training")
            else:
                train, test = ml_df.randomSplit([0.8, 0.2], 42)

                model = RandomForestClassifier(
                    featuresCol="features",
                    labelCol="price_direction",
                    numTrees=20,
                    maxDepth=5,
                    seed=42
                ).fit(train)

                preds = model.transform(test)

                acc = MulticlassClassificationEvaluator(
                    labelCol="price_direction",
                    predictionCol="prediction",
                    metricName="accuracy"
                ).evaluate(preds)

                print(f"✓ ML Accuracy = {acc:.4f}")

        except Exception as e:
            print(f"❌ ML Error: {e}")
            traceback.print_exc()

    # ----------------------------------------------------------
    # 3) GOLD KPIs
    # ----------------------------------------------------------
    try:
        print("Updating GOLD KPIs...")

        df_gold = df.groupBy("symbol").agg(
            avg("price").alias("avg_price"),
            sum_("volume").alias("total_volume"),
            avg("volatility").alias("avg_volatility"),
            (
                sum_(when(col("price_direction") == 1, 1).otherwise(0)) /
                count("*") * 100
            ).alias("pct_upward")
        )

        df_gold.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(GOLD_PATH)

        print("✓ Gold updated")

    except Exception as e:
        print(f"❌ Gold Error: {e}")
        traceback.print_exc()

    df.unpersist()


# ====================================================================
# MAIN STREAMING PIPELINE
# ====================================================================
def start_pipeline():

    try:
        print("Starting streaming pipeline...")

        # -------------------- BRONZE -------------------------
        kafka_df = (
            spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", "stock_fin")
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .option("maxOffsetsPerTrigger", 100)
                .load()
        )

        bronze_df = kafka_df.selectExpr("CAST(value AS STRING) AS json") \
            .select(from_json(col("json"), schema).alias("data")).select("data.*")

        # Start Bronze stream
        bronze_query = bronze_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", CKPT_BRONZE) \
            .start(BRONZE_PATH)

        # -------------------- SILVER -------------------------
        # Convert timestamp string to proper TimestampType ONCE here
        silver_df = (
            bronze_df
                .withColumn("timestamp", to_timestamp(col("timestamp")))
                .filter(col("symbol").isNotNull())
                .filter(col("price") > 0)
                .filter(col("volume") >= 0)
                .filter(col("timestamp").isNotNull())  # Filter out failed conversions
        )

        silver_query = silver_df.writeStream \
            .foreachBatch(process_silver_batch) \
            .option("checkpointLocation", CKPT_SILVER) \
            .trigger(processingTime="10 seconds") \
            .start()

        print("✓ Pipeline Running...")
        print(f"Bronze Path: {BRONZE_PATH}")
        print(f"Silver Path: {SILVER_PATH}")
        print(f"Gold Path: {GOLD_PATH}")
        
        spark.streams.awaitAnyTermination()

    except Exception as e:
        print(f"❌ Stream Error: {e}")
        traceback.print_exc()


# ====================================================================
# MAIN
# ====================================================================
if __name__ == "__main__":
    import sys
    
    # Check if cleanup is requested
    if len(sys.argv) > 1 and sys.argv[1] == "--cleanup":
        cleanup_corrupted_delta()
        print("\nCleanup complete. Restart the container to begin fresh.")
        sys.exit(0)
    
    print("Waiting for services to be ready...")
    time.sleep(10)
    
    print("Starting pipeline...")
    start_pipeline()