from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lead
import os
import json
import time

# --- Configuration ---
spark = SparkSession.builder \
    .appName("PriceModelTraining_MLlib") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

SILVER_PATH = "/app/storage/delta-lake/silver_stocks"
MODEL_PATH = "/app/storage/models/price_prediction_mllib"
METRICS_PATH = "/app/storage/metrics.json"

if not os.path.exists(SILVER_PATH):
    print(f"⚠️ No data found at {SILVER_PATH}. Exiting.")
    spark.stop()
    exit()

# 1. Load ALL Data
df = spark.read.format("delta").load(SILVER_PATH)

# 2. Feature Engineering
# We define "All Data" as every row where we know the 'future_price'
windowSpec = Window.partitionBy("symbol").orderBy("timestamp")
train_df = df.withColumn("future_price", lead("price", 1).over(windowSpec))

# dropna() removes only the very last row of each symbol (since it has no future yet)
# This keeps 99.9% of your history for training.
train_df_clean = train_df.select("symbol", "price", "future_price", "timestamp").dropna()

# 3. Vectorize
assembler = VectorAssembler(inputCols=["price"], outputCol="features")
final_data = assembler.transform(train_df_clean)

# Count total available rows
total_rows = final_data.count()
print(f"🧠 Total Data Points Available: {total_rows}")

# 4. PHASE 1: Validation (Calculate Metrics)
# We still split temporarily just to see how good the model is
train_split, test_split = final_data.randomSplit([0.8, 0.2], seed=42)

rf = RandomForestRegressor(featuresCol="features", labelCol="future_price", numTrees=50)

print("📊 Phase 1: Validating Model Performance (80/20 Split)...")
validation_model = rf.fit(train_split)
predictions = validation_model.transform(test_split)

evaluator_rmse = RegressionEvaluator(labelCol="future_price", predictionCol="prediction", metricName="rmse")
evaluator_r2 = RegressionEvaluator(labelCol="future_price", predictionCol="prediction", metricName="r2")

rmse = evaluator_rmse.evaluate(predictions)
r2 = evaluator_r2.evaluate(predictions)

print(f"   📉 RMSE Score: {rmse}")
print(f"   📊 Accuracy (R2): {r2}")

# 5. PHASE 2: Production Training (Train on 100%)
# Now that we have the score, we retrain on EVERYTHING so the file we save is smarter
print(f"🚀 Phase 2: Retraining Final Model on ALL {total_rows} rows...")
final_model = rf.fit(final_data)

# 6. Save the 100% Model
print(f"💾 Saving Production Model to {MODEL_PATH}...")
final_model.write().overwrite().save(MODEL_PATH)

# 7. Save Metrics
metrics_data = {
    "rmse": round(rmse, 4),
    "r2": round(r2 * 100, 2),
    "rows_trained": total_rows, # Dashboard will now show the full count
    "last_trained": time.strftime("%Y-%m-%d %H:%M:%S")
}

with open(METRICS_PATH, "w") as f:
    json.dump(metrics_data, f)

print("✅ Model Updated Successfully.")
spark.stop()
