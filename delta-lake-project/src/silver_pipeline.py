import os
import numpy as np
import tensorflow as tf

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sha2,
    concat,
    current_timestamp,
    row_number,
    log,
    lag,
    collect_list
)
from pyspark.sql.window import Window

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


# ==================================================
# CONFIGURATION
# ==================================================
base_path = os.getcwd()

delta_path_bronze = "file://" + os.path.join(
    base_path, "finance_lake/bronze/raw_events"
)
delta_path_silver = "file://" + os.path.join(
    base_path, "finance_lake/silver/cleaned_events"
)
checkpoint_path_silver = "file://" + os.path.join(
    base_path, "checkpoints/silver_gru_exec"
)


# ==================================================
# SPARK INITIALIZATION
# ==================================================
builder = (
    SparkSession.builder
    .appName("SilverLayerGRUExecution")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .config("spark.sql.shuffle.partitions", "4")  # reduce noise
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")  # 🔇 silence Spark spam


# ==================================================
# 1. GRU MODEL (SIMULATED)
# ==================================================
def get_gru_prediction(feature_sequence):
    if feature_sequence is None or len(feature_sequence) < 5:
        return 0.0

    inputs = np.array(feature_sequence).reshape(1, len(feature_sequence), 1)
    return float(np.mean(inputs) * 1.1)


predict_udf = spark.udf.register("predict_gru", get_gru_prediction)


# ==================================================
# 2. SILVER PROCESSING + GRU INFERENCE
# ==================================================
def process_silver_gru_batch(df_micro_batch, epoch_id):

    if df_micro_batch.count() == 0:
        print(f"[Epoch {epoch_id}] Empty batch")
        return

    print(f"\n=== Processing batch {epoch_id} ===")

    # ----------------------------------------------
    # A. CLEANING & COLUMN NORMALIZATION
    # ----------------------------------------------
    df_cleaned = (
        df_micro_batch
        .select(
            sha2(
                concat(col("symbol"), col("timestamp"), col("price")),
                256
            ).alias("event_id"),
            col("symbol"),
            col("price"),
            col("volume"),
            col("timestamp"),
            col("ingestion_time").alias("bronze_ingestion_time")
        )
        .filter(col("price") > 0)
        .withColumn("silver_processing_time", current_timestamp())
    )

    dedup_window = Window.partitionBy("event_id").orderBy(col("timestamp").desc())

    df_unique = (
        df_cleaned
        .withColumn("rank", row_number().over(dedup_window))
        .filter(col("rank") == 1)
        .drop("rank")
    )

    # ----------------------------------------------
    # B. FEATURE ENGINEERING
    # ----------------------------------------------
    symbol_window = Window.partitionBy("symbol").orderBy("timestamp")

    df_features = (
        df_unique
        .withColumn("prev_price", lag("price", 1).over(symbol_window))
        .withColumn("log_return", log(col("price") / col("prev_price")))
        .fillna(0)
    )

    sequence_window = (
        Window.partitionBy("symbol")
        .orderBy("timestamp")
        .rowsBetween(-10, 0)
    )

    df_sequences = df_features.withColumn(
        "log_return_sequence",
        collect_list("log_return").over(sequence_window)
    )

    df_predictions = df_sequences.withColumn(
        "gru_prediction",
        predict_udf(col("log_return_sequence"))
    )

    # ----------------------------------------------
    # C. SHOW REAL RESULTS (IMPORTANT)
    # ----------------------------------------------
    df_predictions.select(
        "symbol",
        "timestamp",
        "price",
        "gru_prediction"
    ).orderBy(col("timestamp").desc()).show(5, truncate=False)

    # ----------------------------------------------
    # D. WRITE TO DELTA (SCHEMA EVOLUTION)
    # ----------------------------------------------
    try:
        delta_silver = DeltaTable.forPath(spark, delta_path_silver)

        delta_silver.alias("target").merge(
            df_predictions.alias("source"),
            "target.event_id = source.event_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

    except Exception:
        (
            df_predictions
            .write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(delta_path_silver)
        )


# ==================================================
# 3. START STREAM
# ==================================================
df_bronze_stream = (
    spark.readStream
    .format("delta")
    .load(delta_path_bronze)
)

query = (
    df_bronze_stream
    .writeStream
    .foreachBatch(process_silver_gru_batch)
    .option("checkpointLocation", checkpoint_path_silver)
    .start()
)

query.awaitTermination()

