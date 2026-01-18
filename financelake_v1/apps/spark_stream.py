from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressionModel
import os
import time
import shutil

from apps.schema import STOCK_SCHEMA

# ---------------------------------------------------------
# 1. Configuration
# ---------------------------------------------------------
MODEL_PATH = "/app/storage/models/price_prediction_mllib"
CHECKPOINT_PATH = "/app/storage/checkpoints/gold_predictions"
GOLD_OUTPUT_PATH = "/app/storage/delta-lake/gold_predictions"

# Global variable to track the running stream
current_stream = None
last_model_time = 0

# ---------------------------------------------------------
# 2. Initialize Spark
# ---------------------------------------------------------
spark = SparkSession.builder \
    .appName("StockMedallionStreaming_Pro") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------
# 3. Base Data Stream (Bronze -> Silver)
# ---------------------------------------------------------
# Validating and cleaning data always happens, regardless of model status
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stocks") \
    .option("failOnDataLoss", "false") \
    .load()

silver_df = df_kafka.select(from_json(col("value").cast("string"), STOCK_SCHEMA).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp")))

# Write Silver (Always running in background)
# We return this query object but we don't restart it; it just runs.
silver_query = silver_df.writeStream.format("delta") \
    .option("checkpointLocation", "/app/storage/checkpoints/silver") \
    .outputMode("append") \
    .start("/app/storage/delta-lake/silver_stocks")

print("✅ Silver Stream started.")

# ---------------------------------------------------------
# 4. The "Pro" Manager Function
# ---------------------------------------------------------
def get_model_timestamp():
    """Returns the last modified time of the model directory."""
    try:
        return os.path.getmtime(MODEL_PATH)
    except OSError:
        return 0

def run_gold_stream_logic():
    global current_stream, last_model_time

    print("--- 🧠 AI Model Monitor Active ---")
    
    while True:
        try:
            # 1. Check if model exists
            if not os.path.exists(MODEL_PATH):
                print("⏳ Waiting for model file...", end="\r")
                time.sleep(10)
                continue

            # 2. Check file timestamp
            current_mod_time = get_model_timestamp()

            # 3. Detect Change (New Model or First Run)
            if current_mod_time > last_model_time:
                print(f"\n⚡ New Model Detected! (Timestamp: {current_mod_time})")
                
                # Stop existing stream if running
                if current_stream is not None and current_stream.isActive:
                    print("🛑 Stopping current prediction stream to reload model...")
                    current_stream.stop()
                    current_stream.awaitTermination() # Wait for clean stop

                print("🔄 Loading new model...")
                try:
                    # Load the new model
                    loaded_model = RandomForestRegressionModel.load(MODEL_PATH)
                    assembler = VectorAssembler(inputCols=["price"], outputCol="features")

                    # Apply Transformation
                    # Note: We must re-apply the transformation logic to the silver_df
                    predictions_df = loaded_model.transform(assembler.transform(silver_df)) \
                        .select("symbol", "price", col("prediction").alias("prediction"), "timestamp")

                    # Start the Stream
                    print("🚀 Starting Gold Stream with updated model...")
                    current_stream = predictions_df.writeStream.format("delta") \
                        .outputMode("append") \
                        .option("checkpointLocation", CHECKPOINT_PATH) \
                        .start(GOLD_OUTPUT_PATH)
                    
                    # Update tracker
                    last_model_time = current_mod_time
                    print("✅ Stream is Live.")

                except Exception as e:
                    print(f"❌ Error loading model (Retrying in 10s): {e}")
                    # Reset time so it tries again next loop
                    last_model_time = 0 
            
            # Sleep before next check
            time.sleep(30)

        except KeyboardInterrupt:
            print("Stopping...")
            break
        except Exception as e:
            print(f"Critical Error in Monitor: {e}")
            time.sleep(10)

# ---------------------------------------------------------
# 5. Execution
# ---------------------------------------------------------
if __name__ == "__main__":
    run_gold_stream_logic()