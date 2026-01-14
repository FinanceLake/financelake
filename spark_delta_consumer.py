#!/usr/bin/env python3
"""
Spark Structured Streaming consumer (v2)

- Bronze: append raw messages to Delta
- Silver: clean and append
- Gold: compute 30s windows with 10s slide, compute KPIs and write/merge into Delta

This file follows the request to implement process_gold + compute_kpis and store
results in the Delta `gold` layer under `data_output/gold`.
"""
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
from delta.tables import DeltaTable

# -------------------------------
# Environment variables
# -------------------------------
DATA_OUTPUT_DIR = os.environ.get("DATA_OUTPUT_DIR", "./data_output")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "data_stock")
TRIGGER_INTERVAL = int(os.environ.get("TRIGGER_INTERVAL", "10"))  # seconds

BRONZE_PATH = os.path.join(DATA_OUTPUT_DIR, "bronze")
SILVER_PATH = os.path.join(DATA_OUTPUT_DIR, "silver")
GOLD_PATH = os.path.join(DATA_OUTPUT_DIR, "gold")

# -------------------------------
# Spark session with Delta support
# -------------------------------
spark = SparkSession.builder \
    .appName("SparkDeltaConsumer_v2") \
    .master("local[4]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------
# JSON schema for Kafka messages
# -------------------------------
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("volume", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("open", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("close", FloatType(), True)
])

# -------------------------------
# Read from Kafka
# -------------------------------
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(F.from_json(F.col("json_str"), schema).alias("data")) \
    .select("data.*")

# -------------------------------
# Bronze: raw data write
# -------------------------------
bronze_query = json_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", os.path.join(BRONZE_PATH, "_checkpoints")) \
    .trigger(processingTime=f"{TRIGGER_INTERVAL} seconds") \
    .start(BRONZE_PATH)

# -------------------------------
# Silver: clean, filter, safe numeric casting
# -------------------------------
def clean_silver(df):
    df = df.dropna(subset=["symbol", "price", "volume", "timestamp"]) \
        .filter((col("price") > 0) & (col("volume") >= 0))

    df = df.withColumn("open", F.when(col("open").isNull(), col("price")).otherwise(col("open"))) \
           .withColumn("high", F.when(col("high").isNull(), col("price")).otherwise(col("high"))) \
           .withColumn("low", F.when(col("low").isNull(), col("price")).otherwise(col("low"))) \
           .withColumn("close", F.when(col("close").isNull(), col("price")).otherwise(col("close")))

    df = df.withColumn(
        "volume",
        F.when(col("volume").cast("string").rlike("^[0-9]+$"), col("volume").cast("integer")).otherwise(0)
    ).withColumn(
        "price",
        F.when(col("price").cast("string").rlike("^[0-9]+(\\.[0-9]+)?$"), col("price").cast("double")).otherwise(0.0)
    ).withColumn("open", col("open").cast("double")) \
     .withColumn("high", col("high").cast("double")) \
     .withColumn("low", col("low").cast("double")) \
     .withColumn("close", col("close").cast("double"))

    return df

silver_df = clean_silver(json_df)

silver_query = silver_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", os.path.join(SILVER_PATH, "_checkpoints")) \
    .trigger(processingTime=f"{TRIGGER_INTERVAL} seconds") \
    .start(SILVER_PATH)

# -------------------------------
# Gold: process with foreachBatch
# -------------------------------
def compute_kpis(agg_df):
    """
    Compute additional KPIs: price_momentum and bb_position.
    Returns a DataFrame with the new columns added.
    """
    if agg_df.rdd.isEmpty():
        return agg_df

    from pyspark.sql.window import Window

    w = Window.partitionBy("symbol").orderBy("window_end")

    return agg_df.withColumn(
        "price_momentum",
        F.col("avg_price") - F.lag("avg_price", 1).over(w)
    ).withColumn(
        "bb_position",
        (F.col("avg_price") - F.col("min_price")) /
        (F.col("max_price") - F.col("min_price") + F.lit(0.01))
    )


def process_gold(df_raw, batch_id):
    """
    Implements the requested 30s window aggregated KPIs with 10s slide,
    filters closed windows and writes/merges into Delta GOLD_PATH.
    """
    try:
        print(f"[process_gold] batch_id={batch_id} - start")

        output = DATA_OUTPUT_DIR
        os.makedirs(os.path.join(output, "gold"), exist_ok=True)

        if df_raw.rdd.isEmpty():
            print(f"[process_gold] batch_id={batch_id} - raw empty, skipping")
            return

        clean_df = clean_silver(df_raw)
        if clean_df.rdd.isEmpty():
            print(f"[process_gold] batch_id={batch_id} - cleaned empty, skipping")
            return

        # 30s window, 10s slide
        agg_df = clean_df.withWatermark("timestamp", "30 seconds").groupBy(
            F.window("timestamp", "30 seconds", "10 seconds"),
            F.col("symbol")
        ).agg(
            F.avg("price").alias("avg_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.sum("volume").alias("total_volume"),
            F.count("price").alias("record_count"),
            F.stddev("price").alias("price_volatility")
        )

        agg_df = agg_df.select(
            F.col("symbol"),
            F.col("avg_price"),
            F.col("min_price"),
            F.col("max_price"),
            F.col("total_volume"),
            F.col("record_count"),
            F.col("price_volatility"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end")
        )

        # Only closed windows
        final_windows = agg_df.filter(F.col("window_end") <= F.current_timestamp())

        if final_windows.rdd.isEmpty():
            print(f"[process_gold] batch_id={batch_id} - no closed windows, skipping")
            return

        kpi_df = compute_kpis(final_windows)

        kpi_count = kpi_df.count()
        print(f"[process_gold] batch_id={batch_id} - KPI rows={kpi_count}")

        # merge on symbol + window_start
        if DeltaTable.isDeltaTable(spark, GOLD_PATH):
            try:
                delta_table = DeltaTable.forPath(spark, GOLD_PATH)
            except Exception as ex:
                print(f"[process_gold] batch_id={batch_id} - could not load existing gold table: {ex}")
                delta_table = None

            if delta_table is not None:
                merge_cond = "tgt.symbol = src.symbol AND tgt.window_start = src.window_start"
                delta_table.alias("tgt").merge(
                    kpi_df.alias("src"),
                    merge_cond
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                print(f"[process_gold] batch_id={batch_id} - merge executed")
            else:
                kpi_df.write.format("delta").mode("overwrite").partitionBy("symbol").save(GOLD_PATH)
                print(f"[process_gold] batch_id={batch_id} - initial write complete (fallback)")
        else:
            kpi_df.write.format("delta").mode("overwrite").partitionBy("symbol").save(GOLD_PATH)
            print(f"[process_gold] batch_id={batch_id} - initial write complete")

    except Exception as e:
        print(f"[process_gold] batch_id={batch_id} - error: {e}")


# start gold stream: use foreachBatch
gold_query = silver_df.writeStream \
    .for    python3 -c "import socket; print('pypi.org ->', socket.gethostbyname('pypi.org'))" || true
    ping -c 2 8.8.8.8 || true
    curl -I https://query1.finance.yahoo.com/v7/finance/download/AAPL  -s -o /dev/null -w "%{http_code}\n" || trueeachBatch(process_gold) \
    .option("checkpointLocation", os.path.join(GOLD_PATH, "_checkpoints")) \
    .trigger(processingTime=f"{TRIGGER_INTERVAL} seconds") \
    .start()

# await termination
spark.streams.awaitAnyTermination()
