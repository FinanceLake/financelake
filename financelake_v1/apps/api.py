from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
import os
import asyncio
import json
from contextlib import asynccontextmanager

# --- Configuration ---
GOLD_PATH = "/app/storage/delta-lake/gold_predictions"
METRICS_PATH = "/app/storage/metrics.json"
DASHBOARD_PATH = "/app/dashboard"

# --- Global Cache ---
CACHE = {
    "latest_data": [],
    "metrics": {"rmse": 0, "r2": 0, "last_trained": "Pending"}
}

# --- Initialize Spark ---
spark = SparkSession.builder \
    .appName("DashboardAPI") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.ui.enabled", "false") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# --- Background Worker ---
async def update_cache_loop():
    """Runs in the background to fetch data from Spark/Disk every few seconds."""
    global CACHE
    print("🚀 Background Data Refresher Started")
    
    while True:
        try:
            # 1. Fetch Gold Data (Spark)
            if os.path.exists(GOLD_PATH):
                # FIX: Fetch 5000 rows so we have HISTORY for the chart AND latest data for cards
                df = await asyncio.to_thread(
                    lambda: spark.read.format("delta").load(GOLD_PATH)
                    .orderBy(desc("timestamp"))
                    .limit(5000) 
                    .toPandas()
                )
                
                if not df.empty:
                    df['timestamp'] = df['timestamp'].astype(str)
                    
                    # --- CRITICAL FIX ---
                    # We do NOT drop duplicates here anymore.
                    # We send the full history so the Chart can draw lines.
                    # The Frontend (script.js) is smart enough to extract unique values for the cards.
                    CACHE["latest_data"] = df[['symbol', 'price', 'prediction', 'timestamp']].to_dict(orient='records')

            # 2. Fetch Metrics (JSON File)
            if os.path.exists(METRICS_PATH):
                with open(METRICS_PATH, "r") as f:
                    CACHE["metrics"] = json.load(f)

        except Exception as e:
            print(f"⚠️ Cache Update Error: {e}")

        # Wait 2 seconds before next update
        await asyncio.sleep(2)

# --- Lifecycle Manager ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(update_cache_loop())
    yield

app = FastAPI(lifespan=lifespan)

# --- Middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Endpoints ---

@app.get("/api/latest")
async def get_latest():
    return CACHE["latest_data"]

@app.get("/api/metrics")
async def get_metrics():
    return CACHE["metrics"]

# --- Serve Dashboard ---
if os.path.exists(DASHBOARD_PATH):
    app.mount("/static", StaticFiles(directory=DASHBOARD_PATH), name="static")

@app.get("/")
async def read_root():
    index_file = f"{DASHBOARD_PATH}/index.html"
    if os.path.exists(index_file):
        return FileResponse(index_file)
    return {"error": "Dashboard file not found."}