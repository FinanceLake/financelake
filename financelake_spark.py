#!/usr/bin/env python3
"""
Complete FinanceLake Architecture with Apache Spark, Delta Lake, Structured Streaming, Spark SQL, and MLlib
- Batch processing: Historical data ingestion from Alpha Vantage API and processing to Bronze/Silver/Gold
- Streaming: Real-time ingestion simulation (rate source for demo; adapt to Kafka for real-time producer)
- SQL: Queries on Gold tables
- MLlib: Batch training + real-time inference in streaming pipeline

Requirements:
- pip install delta-spark alpha-vantage pandas pyspark
- Get free API key from https://www.alphavantage.co/support/#api-key
- Note: Free tier limited to 25 requests/day (as of 2026). For batch, fetching 5 symbols = ~5 calls (compact daily).
  If limit exceeded, script will skip or use partial data.
- Run with: python financelake_spark.py
"""
import pandas as pd
from datetime import datetime, timedelta
import time  # For rate limiting
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from delta import configure_spark_with_delta_pip
from alpha_vantage.timeseries import TimeSeries  # alpha-vantage library

# === CONFIGURATION ===
ALPHA_VANTAGE_API_KEY = 'R08DMROVT4UV959W'  # <<< REPLACE WITH YOUR FREE KEY
symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']

# Paths
bronze_stocks_path = "data/bronze/stocks"
silver_stocks_path = "data/silver/stocks"
gold_metrics_path = "data/gold/metrics"
gold_summary_path = "data/gold/summary"

# Initialize Spark with Delta Lake
builder = SparkSession.builder \
    .appName("FinanceLakeSpark") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.LocalLogStore") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:3.0.0") # <--- Changed 2.4.0 to 3.0.0

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Schema
schema = StructType([
    StructField("symbol", StringType()),
    StructField("timestamp", StringType()),
    StructField("date", StringType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", LongType()),
    StructField("source", StringType())
])

# 1. Batch Pipeline: Ingest historical data from Alpha Vantage
def batch_ingest_and_process():
    ts = TimeSeries(key=ALPHA_VANTAGE_API_KEY, output_format='pandas')

    stocks_data = []
    for symbol in symbols:
        try:
            # Get daily data (compact = last 100 days)
            data, _ = ts.get_daily(symbol=symbol, outputsize='compact')
            data = data.rename(columns={
                '1. open': 'open',
                '2. high': 'high',
                '3. low': 'low',
                '4. close': 'close',
                '5. volume': 'volume'
            })
            data['symbol'] = symbol
            data['source'] = 'alpha_vantage'
            data = data.reset_index()
            data['date'] = data['date'].astype(str)
            data['timestamp'] = data['date'] + ' 09:30:00'  # Approximate
            data = data[['symbol', 'timestamp', 'date', 'open', 'high', 'low', 'close', 'volume', 'source']]
            stocks_data.append(data)
            print(f"Fetched {len(data)} records for {symbol}")
        except Exception as e:
            print(f"Error fetching {symbol}: {e} (possible rate limit or invalid key)")
        
        time.sleep(12)  # ~5 calls/minute limit; 12s delay for safety with 5 symbols

    if not stocks_data:
        print("No data fetched! Creating empty Bronze.")
        bronze_df = spark.createDataFrame([], schema)
    else:
        pandas_df = pd.concat(stocks_data, ignore_index=True)
        pandas_df['open'] = pandas_df['open'].astype(float)
        pandas_df['high'] = pandas_df['high'].astype(float)
        pandas_df['low'] = pandas_df['low'].astype(float)
        pandas_df['close'] = pandas_df['close'].astype(float)
        pandas_df['volume'] = pandas_df['volume'].astype(int)
        bronze_df = spark.createDataFrame(pandas_df, schema=schema)
        print(f"Batch: Total fetched {bronze_df.count()} records")

    # Write Bronze
    bronze_df.write.format("delta").mode("overwrite").save(bronze_stocks_path)
    print("Batch: Bronze saved")

    # Silver transformations
    silver_df = bronze_df.withColumn("date", to_date("date")) \
                         .withColumn("year", year("date")) \
                         .withColumn("month", month("date")) \
                         .withColumn("price_change", col("close") - col("open")) \
                         .withColumn("price_change_pct", (col("close") - col("open")) / col("open") * 100) \
                         .withColumn("high_low_range", col("high") - col("low"))
    silver_df.write.format("delta").mode("overwrite").save(silver_stocks_path)
    print("Batch: Silver processed")

    # Gold aggregations
    daily_metrics = silver_df.groupBy("symbol", "date", "year", "month") \
                             .agg(first("open").alias("open_price"),
                                  last("close").alias("close_price"),
                                  max("high").alias("daily_high"),
                                  min("low").alias("daily_low"),
                                  sum("volume").alias("total_volume"),
                                  avg("price_change_pct").alias("avg_daily_return")) \
                             .withColumn("price_range", col("daily_high") - col("daily_low"))
    daily_metrics.write.format("delta").mode("overwrite").save(gold_metrics_path)
    print("Batch: Gold metrics created")

    market_summary = daily_metrics.groupBy("date") \
                                  .agg(countDistinct("symbol").alias("num_symbols"),
                                       avg("close_price").alias("avg_market_price"),
                                       sum("total_volume").alias("total_market_volume"),
                                       avg("avg_daily_return").alias("avg_market_return"))
    market_summary.write.format("delta").mode("overwrite").save(gold_summary_path)
    print("Batch: Gold summary created")

# 2. Streaming Pipeline (demo with rate source)
def streaming_pipeline():
    streaming_df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
    streaming_df = streaming_df.withColumn("symbol", lit("AAPL")) \
                               .withColumn("timestamp", current_timestamp().cast(StringType())) \
                               .withColumn("date", current_date().cast(StringType())) \
                               .withColumn("open", rand() * 100 + 100) \
                               .withColumn("high", col("open") + rand() * 10) \
                               .withColumn("low", col("open") - rand() * 10) \
                               .withColumn("close", col("open") + rand() * 5 - 2.5) \
                               .withColumn("volume", (rand() * 1000000).cast(LongType())) \
                               .withColumn("source", lit("simulated_stream"))

    bronze_query = streaming_df.writeStream.format("delta") \
                                       .outputMode("append") \
                                       .option("checkpointLocation", "checkpoints/bronze") \
                                       .start(bronze_stocks_path)

    bronze_stream = spark.readStream.format("delta").load(bronze_stocks_path)
    silver_stream = bronze_stream.withColumn("date", to_date("date")) \
                                 .withColumn("year", year("date")) \
                                 .withColumn("month", month("date")) \
                                 .withColumn("price_change", col("close") - col("open")) \
                                 .withColumn("price_change_pct", (col("close") - col("open")) / col("open") * 100) \
                                 .withColumn("high_low_range", col("high") - col("low"))
    silver_query = silver_stream.writeStream.format("delta") \
                                        .outputMode("append") \
                                        .option("checkpointLocation", "checkpoints/silver") \
                                        .start(silver_stocks_path)

    silver_stream_gold = spark.readStream.format("delta").load(silver_stocks_path)
    gold_stream = silver_stream_gold.withWatermark("timestamp", "1 minute") \
                                    .groupBy(window("timestamp", "5 minutes"), "symbol") \
                                    .agg(avg("close").alias("avg_close"),
                                         sum("volume").alias("total_volume"))
    gold_query = gold_stream.writeStream.format("delta") \
                                    .outputMode("append") \
                                    .option("checkpointLocation", "checkpoints/gold") \
                                    .start(gold_metrics_path)

    bronze_query.awaitTermination()
    silver_query.awaitTermination()
    gold_query.awaitTermination()

# 3. Spark SQL
def run_sql_queries():
    if spark.catalog.tableExists("market_summary"):
        spark.sql("DROP TABLE IF EXISTS market_summary")
    spark.read.format("delta").load(gold_summary_path).createOrReplaceTempView("market_summary")
    result = spark.sql("SELECT date, avg_market_price FROM market_summary ORDER BY date DESC LIMIT 10")
    result.show()
    print("SQL: Recent market summaries queried")

# 4. MLlib Pipeline
def mllib_pipeline():
    silver_df = spark.read.format("delta").load(silver_stocks_path)
    if silver_df.rdd.isEmpty():
        print("No data for ML training!")
        return

    assembler = VectorAssembler(inputCols=["open", "high", "low", "volume"], outputCol="features")
    lr = LinearRegression(featuresCol="features", labelCol="close")
    pipeline = Pipeline(stages=[assembler, lr])
    model = pipeline.fit(silver_df)
    model.write().overwrite().save("models/spark_lr_model")
    print("MLlib: Model trained")

    predictions = model.transform(silver_df)
    evaluator = RegressionEvaluator(labelCol="close", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print(f"MLlib: RMSE on training = {rmse}")

    # Streaming inference
    silver_stream = spark.readStream.format("delta").load(silver_stocks_path)
    loaded_model = PipelineModel.load("models/spark_lr_model")
    stream_pred = loaded_model.transform(silver_stream)
    pred_query = stream_pred.select("symbol", "date", "close", "prediction") \
                            .writeStream.format("console") \
                            .outputMode("append") \
                            .option("checkpointLocation", "checkpoints/predictions") \
                            .start()
    print("Streaming + MLlib: Predictions running")
    pred_query.awaitTermination()

if __name__ == "__main__":
    batch_ingest_and_process()   # Fetch from Alpha Vantage + process
    run_sql_queries()
    mllib_pipeline()             # Train + streaming inference
    streaming_pipeline()         # Simulated streaming (Ctrl+C to stop)

spark.stop()