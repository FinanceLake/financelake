import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, stddev, count, lag, when
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from delta import configure_spark_with_delta_pip

# --- CONFIGURATION ---
base_path = os.getcwd()

# Source: Silver Delta Table (Input from Person 2)
delta_path_silver = "file://" + os.path.join(base_path, "finance_lake/silver/cleaned_events")
# Destination: Gold Delta Table (Output for Person 3)
delta_path_gold = "file://" + os.path.join(base_path, "finance_lake/gold/stock_features")

print(f"!!! GOLD TABLE DESTINATION: {delta_path_gold} !!!")

# --- INITIALIZE SPARK SESSION WITH DELTA SUPPORT ---
builder = SparkSession.builder \
    .appName("DeltaLakeGoldMLPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.sql.shuffle.partitions", "4")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# -------------------------------------------------------------
# STEP 1: READ SILVER LAYER (Batch Mode)
# -------------------------------------------------------------
print("--- 1. Reading Cleaned Data from Silver Layer ---")
try:
    df_silver = spark.read.format("delta").load(delta_path_silver)
except Exception as e:
    print(f"❌ ERROR: Could not read Silver Table. Ensure Person 2 ran the silver_pipeline.py. Error: {e}")
    spark.stop()
    exit()

if df_silver.count() < 100:
    print("⚠️ Not enough data for robust ML training. Proceeding for demo purposes.")

# -------------------------------------------------------------
# STEP 2: AGGREGATION (Gold Layer KPI Generation)
# -------------------------------------------------------------
print("--- 2. Aggregating to 5-Minute Windows (KPIs) ---")
df_gold_kpis = df_silver \
    .groupBy(window(col("timestamp"), "5 minutes").alias("window"), col("symbol")) \
    .agg(
        avg("price").alias("avg_price"),
        stddev("price").alias("volatility"),
        count("volume").alias("trade_count")
    ) \
    .filter(col("volatility").isNotNull())

# -------------------------------------------------------------
# STEP 3: FEATURE ENGINEERING (Target Label Creation)
# -------------------------------------------------------------
print("--- 3. Feature Engineering: Creating Target Label ---")
window_spec = Window.partitionBy("symbol").orderBy("window.start")
df_labeled = df_gold_kpis.withColumn("prev_price", lag("avg_price", 1).over(window_spec))
df_labeled = df_labeled.withColumn("label", when(col("avg_price") > col("prev_price"), 1.0).otherwise(0.0))
df_ml_ready = df_labeled.dropna(subset=['prev_price'])

# -------------------------------------------------------------
# STEP 4: VECTORIZATION (Preparing for MLlib)
# -------------------------------------------------------------
print("--- 4. Vectorization ---")
indexer = StringIndexer(inputCol="symbol", outputCol="symbol_index")
indexer_model = indexer.fit(df_ml_ready)
df_indexed = indexer_model.transform(df_ml_ready)

assembler = VectorAssembler(
    inputCols=["volatility", "symbol_index", "avg_price"],
    outputCol="features"
)
data_for_ml = assembler.transform(df_indexed)

# -------------------------------------------------------------
# STEP 5: TRAIN MODEL AND PREDICT ON FULL DATA
# -------------------------------------------------------------
print("--- 5. Training Logistic Regression Model ---")
train, test = data_for_ml.randomSplit([0.8, 0.2], seed=42)

if train.count() == 0 or test.count() == 0:
    print("❌ Not enough data to split for ML.")
    spark.stop()
    exit()

lr = LogisticRegression(featuresCol="features", labelCol="label")
model = lr.fit(train)
    
# C. Predict on Test Set (for accuracy evaluation only)
predictions_test = model.transform(test)
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy"
)
accuracy = evaluator.evaluate(predictions_test)
print(f"\n📊 Model Accuracy on Test Set: {accuracy:.2%}")
predictions_test.select("symbol", "avg_price", "label", "prediction").show(15)

# 1. GENERATE PREDICTIONS ON THE *FULL* DATASET
df_gold_predicted_full = model.transform(data_for_ml)

# -------------------------------------------------------------
# STEP 6: WRITE GOLD LAYER (Final Consumption Table) - CRITICAL FIX
# -------------------------------------------------------------
print(f"--- 6. Writing final aggregated data to Gold Layer: {delta_path_gold} ---")

# 2. SELECT THE FINAL COLUMNS, INCLUDING 'prediction'
df_final_gold = df_gold_predicted_full.select(
    "window.start", "window.end", "symbol",
    "avg_price", "volatility", "trade_count",
    "label", "features",
    "prediction" # <<< THE PREDICTION COLUMN IS NOW INCLUDED
)

# 3. Write to Delta
df_final_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(delta_path_gold)

print(">> GOLD LAYER WRITE SUCCESS! <<")

# -------------------------------------------------------------
# STEP 7: DEMONSTRATE TIME TRAVEL (For Auditability)
# -------------------------------------------------------------
print("\n--- 7. Demonstrating Delta Time Travel (Audit) ---")
version_history = spark.sql(f"DESCRIBE HISTORY delta.`{delta_path_gold}`").collect()
if len(version_history) > 0:
    last_version = version_history[0].version
    # Ensure there is a previous version to read
    if last_version > 0:
        print(f"Reading Version {last_version - 1} of the Gold Table (Before last overwrite):")
        df_old_version = spark.read.format("delta").option("versionAsOf", last_version - 1).load(delta_path_gold)
        print(f"Count in Previous Version ({last_version - 1}): {df_old_version.count()}")
    else:
        print("No previous version (0) available yet for Time Travel.")


spark.stop()
