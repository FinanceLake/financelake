from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lead, when, percent_rank
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from feature_engineering import transform_data
import logging

logging.basicConfig(
    filename='logs/train_rf.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    filemode='w'
)

def train():
    spark = SparkSession.builder.appName("StockTrainRF") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    
    logging.info("⏳ Reading Gold Data...")
    try:
        df = spark.read.format("delta").load("s3a://finance-lake/lake/gold")
    except:
        logging.error("No Gold data found. Skipping.")
        return

    df = transform_data(df)
    
    w = Window.partitionBy("symbol").orderBy("window_start")
    df = df.withColumn("next_price", lead("avg_price", 1).over(w)) \
           .withColumn("label", when(col("next_price") > col("avg_price"), 1.0).otherwise(0.0)) \
           .na.drop()

    logging.info("⏳ Splitting Data by Time (80% Train / 20% Test)...")
    
    df = df.withColumn("rank", percent_rank().over(w))
    
    train_df = df.filter(col("rank") <= 0.8).drop("rank")
    test_df = df.filter(col("rank") > 0.8).drop("rank")
    
    logging.info(f"   Train Rows: {train_df.count()}")
    logging.info(f"   Test Rows:  {test_df.count()}")

    feature_cols = ["log_returns", "volatility", "log_volume"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=50)
    pipeline = Pipeline(stages=[assembler, rf])

    logging.info("Training Random Forest...")
    logging.info("🚀 Training Random Forest...")
    model = pipeline.fit(train_df)

    
    predictions = model.transform(test_df)
    evaluator = predictions.filter(col("prediction") == col("label"))
    accuracy = evaluator.count() / predictions.count()
    
    logging.info(f"Model Accuracy (Time Split): {accuracy:.2%}")
    logging.info(f"Model Accuracy: {accuracy:.2%}")

    model_path = "s3a://finance-lake/models/stock_rf"
    logging.info(f"Saving model to {model_path}...")
    model.write().overwrite().save(model_path)
    logging.info("Done.")

if __name__ == "__main__":
    train()