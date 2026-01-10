import sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.spark_config import create_spark_session
from pyspark.sql.functions import col, year, month, dayofmonth, current_timestamp
from pyspark.sql.window import Window

BRONZE_PATH = "hdfs://localhost:9000/financelake/bronze"
SILVER_PATH = "hdfs://localhost:9000/financelake/silver"
CHECKPOINT_PATH = "hdfs://localhost:9000/financelake/checkpoints/silver"

MODE = "stream" 

spark = create_spark_session()

if MODE == "batch":
    print("Lecture Bronze en batch")
    df_bronze = spark.read.format("delta").load(BRONZE_PATH)
    print(f"Bronze count = {df_bronze.count()}")

    df_prepared = df_bronze.withColumn("datetime", col("event_time"))

    window_spec = Window.partitionBy("transaction_id").orderBy(col("ingest_time").desc())
    df_dedup = df_prepared.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1).drop("rn")

    silver_df = (
        df_dedup
        .filter(col("transaction_id").isNotNull())
        .filter(col("ticker").isNotNull())
        .filter(col("datetime").isNotNull())
        .withColumn("year", year("datetime"))
        .withColumn("month", month("datetime"))
        .withColumn("day", dayofmonth("datetime"))
        .withColumn("range", col("High") - col("Low"))
        .withColumn("processing_time", current_timestamp())
    )

    print(f"Silver count = {silver_df.count()}")

    silver_df.write.format("delta").mode("overwrite").save(SILVER_PATH)
    print("Silver layer créée en batch")

elif MODE == "stream":
    print("Lecture Bronze en streaming")
    df_bronze_stream = spark.readStream.format("delta").load(BRONZE_PATH)

    df_prepared = df_bronze_stream.withColumn("datetime", col("event_time"))

    df_dedup = df_prepared.dropDuplicates(["transaction_id"])

    silver_df = (
        df_dedup
        .filter(col("transaction_id").isNotNull())
        .filter(col("ticker").isNotNull())
        .filter(col("datetime").isNotNull())
        .withColumn("year", year("datetime"))
        .withColumn("month", month("datetime"))
        .withColumn("day", dayofmonth("datetime"))
        .withColumn("range", col("High") - col("Low"))
        .withColumn("processing_time", current_timestamp())
    )

    query = (
        silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start(SILVER_PATH)
    )

    query.awaitTermination()

else:
    raise ValueError("MODE doit être 'batch' ou 'stream'")
