import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
import plotly.express as px

# --- Configuration ---
# WARNING: Ensure this path is correct for your system!
DELTA_PATH_GOLD = "file:///home/ouissal/lab8/finance_lake/gold/stock_features"
ACCURACY_SCORE = 77.78

def get_spark_session():
    """Configures and returns a Spark Session with Delta Lake support."""
    # Note: Using getOrCreate() will return an existing session if one is already running.
    return SparkSession.builder \
        .appName("StreamlitDeltaDashboard") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .getOrCreate()

# FIX 1: Renamed 'spark' to '_spark' to prevent UnhashableParamError with st.cache_data
@st.cache_data
def load_data(_spark, version): 
    """Loads data from the Gold Delta table, optionally using Time Travel."""
    try:
        import os
        if not os.path.exists(DELTA_PATH_GOLD.replace("file://", "")):
            st.error("Gold Layer Delta table not found! Please run gold_pipeline.py twice.")
            return pd.DataFrame()

        if version == 'Audit (V0)':
            st.warning("Loading AUDIT DATA: Version 0 of the Gold Table.")
            df = _spark.read.format("delta").option("versionAsOf", 0).load(DELTA_PATH_GOLD)
        else:
            st.info("Loading CURRENT DATA: Latest version of the Gold Table.")
            df = _spark.read.format("delta").load(DELTA_PATH_GOLD)

        return df.toPandas()

    except Exception as e:
        st.error(f"Error loading Delta table. Ensure your paths are correct and the Gold table exists. Error: {e}")
        return pd.DataFrame()

# --- Streamlit App Layout ---
st.set_page_config(layout="wide")
st.title("💰 Financial Predictive Insights")

# Initialize Spark
spark = get_spark_session()

# --- Sidebar for Filters and Time Travel (Operational Controls) ---
st.sidebar.header("Operational Controls")

# Time Travel Selector
version_select = st.sidebar.selectbox(
    "Delta Data Version (Time Travel)",
    ['Current (V1)', 'Audit (V0)'],
    index=0,
    help="Select 'Audit (V0)' to demonstrate reading a historical version."
)

# Load data based on selection. Passing the globally defined 'spark' object.
df_pandas = load_data(spark, version_select)

if df_pandas.empty:
    st.stop()

# FIX 2 & 3: Use the CORRECT column names 'end' and 'prediction'
try:
    # Create the plotting column 'window_end_time' from the Gold Layer's 'end' column
    df_pandas['window_end_time'] = pd.to_datetime(df_pandas['end'])
    
    # Check if prediction column is present before proceeding
    if 'prediction' not in df_pandas.columns:
        st.error("FATAL: 'prediction' column is missing. Rerun gold_pipeline.py with the fix applied!")
        st.stop()

except KeyError as e:
    st.error(f"FATAL ERROR: A required column was not found. Error: {e}")
    st.stop()

# Stock Symbol Filter
available_symbols = df_pandas['symbol'].unique()
symbol_filter = st.sidebar.selectbox("Filter by Stock Symbol", available_symbols)

# Apply Symbol Filter
df_filtered = df_pandas[df_pandas['symbol'] == symbol_filter].copy()

# --- Main Dashboard Section ---

# 1. Metric Cards (KPIs and Accuracy)
col1, col2, col3 = st.columns(3)

# Model Accuracy Card
col1.metric("📊 Model Accuracy (Test Set)", f"{ACCURACY_SCORE}%")

# Current Signal Card
latest_prediction = df_filtered['prediction'].iloc[-1]
signal_label = "Price Jump (1.0) 🚀" if latest_prediction == 1.0 else "No Jump (0.0) 📉"
col2.metric("💡 Current 5-Min Prediction", signal_label)

# Data Count Card
col3.metric("🗂 Total Records Loaded", f"{len(df_pandas)} (Gold Layer)")

st.markdown("---")

# 2. Price Trend & Prediction Chart
st.header(f"5-Minute Average Price Trend & Prediction for {symbol_filter}")

# Create the Line Chart for Avg Price
fig = px.line(
    df_filtered,
    x='window_end_time', 
    y='avg_price',
    title="Price Trend with ML Prediction Overlay",
    labels={'avg_price': 'Average Price', 'window_end_time': 'Time Window End'},
    template="plotly_dark"
)

# Overlay Scatter Plot for Price Jumps (where prediction == 1.0)
df_jumps = df_filtered[df_filtered['prediction'] == 1.0]

fig.add_scatter(
    x=df_jumps['window_end_time'],
    y=df_jumps['avg_price'],
    mode='markers',
    marker=dict(size=12, symbol='triangle-up', color='lime'),
    name='ML Prediction: Price Jump'
)

st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# 3. Prediction Detail Table (Audit Trail)
st.header("Recent Predictions & Actual Outcomes (Audit)")
# Use the correct column name 'end' for displaying in the table
st.dataframe(df_filtered[['end', 'avg_price', 'prediction', 'label']].tail(15).sort_values(by='end', ascending=False))

# Optional: Stop Spark Session gracefully
spark.stop()
