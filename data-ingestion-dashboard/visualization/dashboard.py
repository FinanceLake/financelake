import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from datetime import datetime, timedelta
import numpy as np
import joblib
import pandas as pd
import matplotlib.pyplot as plt
from tensorflow.keras.models import load_model
import streamlit as st
from pyspark.sql.functions import col, max, to_date
from config.spark_config import create_spark_session
import py4j.protocol
from delta.tables import DeltaTable

# ================================
# CONSTANTS AND PATHS
# ================================
BRONZE_PATH = "hdfs://localhost:9000/financelake/bronze"
SILVER_PATH = "hdfs://localhost:9000/financelake/silver"
GOLD_PATH = "hdfs://localhost:9000/financelake/gold"
ERROR_LOG_PATH = "hdfs://localhost:9000/financelake/error_logs"
MODEL_DIR = "analytics/models/"
LOOKBACK = 7

# ================================
# THEME MANAGEMENT
# ================================
if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = False

def toggle_theme():
    st.session_state.dark_mode = not st.session_state.dark_mode
    st.rerun()

# ================================
# PAGE CONFIG
# ================================
st.set_page_config(
    page_title="FinanceLake – Ingestion Dashboard",
    page_icon="assets/finace--Lake-logo.jpg",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ================================
# APPLY CSS BASED ON THEME
# ================================
if st.session_state.dark_mode:
    st.markdown("""
        <style>
        body, .main, .block-container { background-color: #0E1117 !important; color: #FAFAFA !important; }
        button { background-color: #1E90FF !important; color: white !important; border: none !important; }
        h1, h2, h3, h4, h5, h6 { color: #FAFAFA !important; }
        .css-1d391kg { color: #FAFAFA !important; }
        </style>
    """, unsafe_allow_html=True)
else:
    st.markdown("""
        <style>
        body, .main, .block-container { background-color: #FFFFFF !important; color: #000000 !important; }
        button { background-color: #1E90FF !important; color: white !important; border: none !important; }
        h1, h2, h3, h4, h5, h6 { color: #000000 !important; }
        .css-1d391kg { color: #000000 !important; }
        </style>
    """, unsafe_allow_html=True)

# ================================
# SPARK SESSION
# ================================
@st.cache_resource
def get_spark():
    return create_spark_session()

spark = get_spark()

# ================================
# DATA LOADING FUNCTIONS
# ================================
def load_bronze():
    return spark.read.format("delta").load(BRONZE_PATH)

def load_gold():
    return spark.read.format("delta").load(GOLD_PATH)

def get_last_ingestion_time(df):
    return df.select(max("ingest_time")).collect()[0][0]

def ingestion_status(last_ingest):
    if last_ingest is None:
        return "❌ No ingestion"
    delay = datetime.now() - last_ingest.replace(tzinfo=None)
    if delay < timedelta(minutes=2):
        return "🟢 Healthy"
    elif delay < timedelta(minutes=10):
        return "🟠 Late"
    else:
        return "🔴 Delayed"

def ingestion_volume(df):
    return (
        df.withColumn("ingest_date", to_date(col("ingest_time")))
          .groupBy("ingest_date")
          .count()
          .orderBy("ingest_date")
          .toPandas()
    )

def latest_bronze_events(df, limit=10):
    rows = (
        df.orderBy(col("ingest_time").desc())
          .limit(limit)
          .collect()
    )
    return pd.DataFrame([r.asDict() for r in rows])

def latest_gold_events(df, limit=10):
    rows = (
        df.select(
            "trading_date",
            "ticker",
            "avg_price",
            "total_volume"
        )
        .orderBy(col("trading_date").desc())
        .limit(limit)
        .toPandas()
    )
    return rows

def predict_ticker_price(ticker_name):
    model_path = f"{MODEL_DIR}model_{ticker_name}.h5"
    scaler_path = f"{MODEL_DIR}scaler_{ticker_name}.gz"
    
    if not os.path.exists(model_path) or not os.path.exists(scaler_path):
        return None, f"No model or scaler found for {ticker_name}"

    df_last = spark.read.format("delta").load(GOLD_PATH) \
        .filter(col("ticker") == ticker_name) \
        .orderBy(col("trading_date").desc()) \
        .limit(LOOKBACK)
    pdf = df_last.toPandas()
    
    if len(pdf) < LOOKBACK:
        return None, f"Not enough data to predict {ticker_name}"
    
    pdf = pdf.iloc[::-1] 
    
    model = load_model(model_path)
    scaler = joblib.load(scaler_path)
    
    input_scaled = scaler.transform(pdf[["avg_price", "total_volume"]])
    X_input = input_scaled.reshape(1, LOOKBACK, 2)
    
    pred_scaled = model.predict(X_input, verbose=0)[0][0]
    
    dummy = np.zeros((1, 2))
    dummy[0, 0] = pred_scaled
    price_pred = scaler.inverse_transform(dummy)[0, 0]
    
    return price_pred, None

# ================================
# NAVBAR WITH LOGO AND THEME TOGGLE
# ================================
navbar_col1, navbar_col2 = st.columns([9,1])
with navbar_col1:
    st.image("assets/finace--Lake-logo.jpg", width=60)
    st.markdown("<h1 style='margin-bottom:0;'>FinanceLake Dashboard</h1>", unsafe_allow_html=True)
with navbar_col2:
    if st.button("🌙" if not st.session_state.dark_mode else "☀️"):
        toggle_theme()

st.markdown("---")

# ================================
# SIDEBAR MENU
# ================================
menu = st.sidebar.radio(
    "Navigation",
    options=["Dashboard", "Latest ingestions", "KPIS", "Error Logs", "Predictions"]
)

# ================================
# MAIN DASHBOARD LOGIC
# ================================
try:
    bronze_df = load_bronze()
    total_records = bronze_df.count()
    last_ingest = get_last_ingestion_time(bronze_df)
    status = ingestion_status(last_ingest)
    volume_df = ingestion_volume(bronze_df)
except Exception as e:
    st.error(f"Failed to load Bronze layer: {e}")
    st.stop()

if menu == "Dashboard":
    st.title("📈 Ingestion Overview (Bronze Layer)")

    c1, c2, c3 = st.columns(3)
    c1.metric(
        "📅 Last Ingestion Time",
        last_ingest.strftime("%Y-%m-%d %H:%M:%S") if last_ingest else "N/A"
    )
    c2.metric(
        "📦 Total Bronze Records",
        f"{total_records:,}"
    )
    c3.metric(
        "⏱ Ingestion Status",
        status
    )

    st.markdown("### 📊 Ingestion Volume by Day")
    if not volume_df.empty:
        volume_df["ingest_date"] = pd.to_datetime(volume_df["ingest_date"])
        volume_df = volume_df.set_index("ingest_date")
        st.area_chart(volume_df["count"])
    else:
        st.info("No ingestion volume data available.")

elif menu == "Latest ingestions":
    st.title(" Latest Ingested Events - Bronze Layer")
    try:
        preview_df = latest_bronze_events(bronze_df)
        st.dataframe(preview_df, use_container_width=True)
    except Exception as e:
        st.warning(f"Preview failed: {e}")

elif menu == "KPIS":
    st.title(" Latest Ingested Events - Gold Layer")
    try:
        gold_df = load_gold()
        preview_gold = latest_gold_events(gold_df)
        if preview_gold.empty:
            st.info("No data available in Gold layer.")
        else:
            st.dataframe(preview_gold, use_container_width=True)
    except Exception as e:
        st.warning(f"Preview failed: {e}")

elif menu == "Error Logs":
    st.title("🚨 Error Logs")
    try:
        if DeltaTable.isDeltaTable(spark, ERROR_LOG_PATH):
            error_df = (
                spark.read.format("delta")
                .load(ERROR_LOG_PATH)
                .orderBy(col("timestamp").desc())
                .limit(20)
                .toPandas()
            )
            if error_df.empty:
                st.success("No ingestion errors 🎉")
            else:
                st.dataframe(error_df, use_container_width=True)
        else:
            st.info("Error log table does not exist or is not initialized yet.")
    except py4j.protocol.Py4JJavaError as e:
        st.error(f"Error reading error logs: {e}")
    except Exception as e:
        st.error(f"Unexpected error: {e}")

elif menu == "Predictions":
    st.title("📈 Price Predictions for Next Day")

    try:
        gold_df = load_gold()
        tickers = gold_df.select("ticker").distinct().toPandas()["ticker"].tolist()
    except Exception as e:
        st.error(f"Failed to load GOLD data: {e}")
        tickers = []

    if not tickers:
        st.info("No tickers found to predict.")
    else:
        for ticker in tickers:
            st.subheader(f"Ticker: {ticker}")
            price_pred, err = predict_ticker_price(ticker)
            if err:
                st.warning(err)
                continue

            st.metric("Predicted Closing Price for Next Day ($)", f"{price_pred:.2f}")

            df_hist = gold_df.filter(col("ticker") == ticker) \
                             .orderBy(col("trading_date").desc()) \
                             .limit(30).toPandas()
            df_hist = df_hist.iloc[::-1]

            plt.figure(figsize=(8, 4))
            plt.plot(df_hist["trading_date"], df_hist["avg_price"], label="Historical Avg Price")
            next_day = pd.to_datetime(df_hist["trading_date"].max()) + pd.Timedelta(days=1)
            plt.scatter(next_day, price_pred, color="red", label="Predicted Price")
            plt.xticks(rotation=45)
            plt.ylabel("Price ($)")
            plt.legend()
            plt.tight_layout()
            st.pyplot(plt)
            plt.clf()

# ================================
# REFRESH BUTTON
# ================================
st.sidebar.markdown("---")
if st.sidebar.button("🔄 Refresh Dashboard"):
    st.cache_data.clear()
    st.rerun()

