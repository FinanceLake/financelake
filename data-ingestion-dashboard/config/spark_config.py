from pyspark.sql import SparkSession
_spark = None

def create_spark_session():
    global _spark

    if _spark is None:
        _spark = SparkSession.builder \
            .appName("FinanceLakeDashboard") \
            .master("local[*]") \
            .config("spark.jars.packages", 
                "io.delta:delta-core_2.12:1.0.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

    return _spark
