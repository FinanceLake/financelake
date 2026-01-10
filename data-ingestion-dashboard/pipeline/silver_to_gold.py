import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.spark_config import create_spark_session
from pyspark.sql.functions import avg, sum, col, to_date

spark = create_spark_session()

SILVER_PATH = "hdfs://localhost:9000/financelake/silver"
GOLD_PATH = "hdfs://localhost:9000/financelake/gold"

df = spark.read.format("delta").load(SILVER_PATH)

gold = (
    df.withColumn("trading_date", to_date(col("datetime")))
      .groupBy("ticker", "trading_date")
      .agg(
          avg("Close").alias("avg_price"),
          sum("Volume").alias("total_volume")
      )
      .orderBy("ticker", "trading_date")
)

gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(GOLD_PATH)

gold.show()

gold.groupBy("ticker").count().show()
