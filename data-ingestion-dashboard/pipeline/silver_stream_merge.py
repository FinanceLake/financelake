
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.spark_config import create_spark_session
from pyspark.sql.functions import col, year, month, dayofmonth, current_timestamp, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

BRONZE_PATH = "hdfs://localhost:9000/financelake/bronze"
SILVER_PATH = "hdfs://localhost:9000/financelake/silver"
CHECKPOINT = "hdfs://localhost:9000/financelake/checkpoints/silver"

MODE = "stream"  

spark = create_spark_session()

def upsert_to_delta(micro_batch_df, batch_id):
    if micro_batch_df.isEmpty():
        print(f"Batch {batch_id}: No data to merge.")
        return

    print(f"Batch {batch_id}: Merge {micro_batch_df.count()} records")

    window_spec = Window.partitionBy("transaction_id").orderBy(col("ingest_time").desc())
    source = micro_batch_df.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1).drop("rn")

    delta_table = DeltaTable.forPath(spark, SILVER_PATH)

    delta_table.alias("t").merge(
        source.alias("s"),
        "t.transaction_id = s.transaction_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

    print(f"Batch {batch_id}: Merge completed")

if MODE == "batch":
    print("Lecture Bronze en batch")
    df_bronze = spark.read.format("delta").load(BRONZE_PATH)
    print(f"Bronze count = {df_bronze.count()}")

    df_prepared = df_bronze.withColumn("datetime", col("event_time"))

    df_transformed = (
        df_prepared
        .withColumn("year", year("datetime"))
        .withColumn("month", month("datetime"))
        .withColumn("day", dayofmonth("datetime"))
        .withColumn("range", col("High") - col("Low"))
        .withColumn("processing_time", current_timestamp())
    )

    upsert_to_delta(df_transformed, batch_id=0)
    print("Silver layer créée en batch avec merge")

elif MODE == "stream":
    print("Lecture Bronze en streaming")
    bronze_stream = spark.readStream.format("delta").load(BRONZE_PATH)

    silver_stream = (
        bronze_stream
        .withColumn("datetime", col("event_time"))
        .withColumn("year", year("datetime"))
        .withColumn("month", month("datetime"))
        .withColumn("day", dayofmonth("datetime"))
        .withColumn("range", col("High") - col("Low"))
        .withColumn("processing_time", current_timestamp())
    )

    query = silver_stream.writeStream \
        .foreachBatch(upsert_to_delta) \
        .option("checkpointLocation", CHECKPOINT) \
        .start()

    print("Streaming Bronze → Silver avec merge lancé…")
    query.awaitTermination()

else:
    raise ValueError("MODE doit être 'batch' ou 'stream'")
