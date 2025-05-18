from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col

# Initialisation SparkSession
spark = SparkSession.builder \
    .appName("KafkaToParquetConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schéma des messages JSON Kafka
schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("timestamp", TimestampType())

# Lecture du stream depuis Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock-data") \
    .option("startingOffsets", "latest") \
    .load()

# Conversion des valeurs Kafka (bytes -> JSON)
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Écriture en Parquet
query = json_df.writeStream \
    .format("parquet") \
    .option("path", "output/parquet_data") \
    .option("checkpointLocation", "output/checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
