from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

# 1. Initialiser la session Spark
spark = SparkSession.builder \
    .appName("FinanceLakeMultiSymbol") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# 2. Définir le schéma des données
schema = """
    symbol STRING,
    price DOUBLE,
    timestamp LONG
"""
# 3. Lire depuis Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "finance-multi-symbol") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Parser les données
parsed_df = df.select(
    col("key").cast("string").alias("symbol"),
    from_json(col("value").cast("string"), schema).alias("data")
).select("symbol", "data.price", "data.timestamp")

# 5. Écrire en Parquet partitionné
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()


query.awaitTermination()
