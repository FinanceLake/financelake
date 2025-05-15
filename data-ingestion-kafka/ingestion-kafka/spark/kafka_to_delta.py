from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaToDelta") \
    .getOrCreate()

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "finance") \
    .load()

df_parsed = df_kafka.selectExpr("CAST(value AS STRING)")

df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "file:///home/amina/FinanceLake/checkpoints/finance") \
    .start("file:///home/amina/FinanceLake/data/finance_delta") \
    .awaitTermination()
