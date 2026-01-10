import streamlit as st
import duckdb
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import time
import os

# --- Configuration ---
st.set_page_config(
    page_title="Real-Time Stock Insight",
    page_icon="📈",
    layout="wide"
)

# CRITICAL CONFIG: Points to MinIO/S3 subfolder
# Spark writes to: s3a://finance-lake/lake/gold
# DuckDB reads:    s3://finance-lake/lake/gold
GOLD_PATH = "s3://finance-lake/lake/gold"
PRED_PATH = "s3://finance-lake/lake/predictions"
REFRESH_SECONDS = 2

# --- Data Loading ---
def get_duckdb_connection():
    """Establishes an in-memory DuckDB connection with S3/MinIO support"""
    con = duckdb.connect(database=':memory:')
    
    # INSTALL S3 SUPPORT
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    # CONFIGURE S3 (Read from Docker Env Vars)
    s3_endpoint = os.getenv("S3_ENDPOINT", "minio:9000").replace("http://", "")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    
    # Set S3 params for MinIO (path style, no SSL)
    con.execute(f"""
        SET s3_endpoint='{s3_endpoint}';
        SET s3_access_key_id='{access_key}';
        SET s3_secret_access_key='{secret_key}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return con

def load_data():
    """Reads Parquet files from MinIO Gold Layer"""
    try:
        con = get_duckdb_connection()
        # DuckDB glob pattern for recursive S3 read
        query = f"""
            SELECT * 
            FROM read_parquet('{GOLD_PATH}/symbol=*/*.parquet', hive_partitioning=true)
        """
        df = con.execute(query).df()
        
        if df.empty:
            return None
            
        return df
    except Exception as e:
        # Swallow 404s during startup, show others
        if "404" not in str(e) and "No files" not in str(e):
             st.error(f"❌ Error connecting to Gold Layer: {e}")
        return None

def load_predictions(symbol):
    """Reads latest ML Predictions for a symbol"""
    try:
        con = get_duckdb_connection()
        # Check if prediction files exist first
        query = f"""
            SELECT * 
            FROM read_parquet('{PRED_PATH}/*.parquet')
            WHERE symbol = '{symbol}'
            ORDER BY prediction_timestamp DESC
            LIMIT 1
        """
        df = con.execute(query).df()
        return df.iloc[0] if not df.empty else None
    except:
        return None # Fail silently if predictions table doesn't exist yet

# --- UI Components ---
def render_kpis(df_symbol, prediction=None):
    """Render Advanced Key Performance Indicators"""
    # Sort by time to get latest
    df_sorted = df_symbol.sort_values("window_end", ascending=False)
    latest = df_sorted.iloc[0]
    
    # Calculate Session Metrics (Aggregates from the loaded dataframe)
    session_high = df_symbol['max_price'].max()
    session_low = df_symbol['min_price'].min()
    
    # --- ROW 1: IMMEDIATE ACTION ---
    st.markdown("### ⚡ Live Market Data")
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    
    with kpi1:
        st.metric(
            label="Latest Price", 
            value=f"${latest['avg_price']:.2f}",
            delta=f"{latest['price_change_pct']:.2f}%" if 'price_change_pct' in latest else None
        )
    
    with kpi2:
        # ML Signal Box
        if prediction is not None:
            sig = prediction['signal'] if 'signal' in prediction else ("BUY" if prediction['probability'] > 0.5 else "SELL")
            prob = prediction['probability']
            color = "normal" if sig == "BUY" else "inverse"
            st.metric("AI Signal (LSTM)", f"{sig} ({prob:.1%})", delta="New Prediction" if prob > 0.6 else None, delta_color=color)
        else:
            st.metric("AI Signal", "Waiting...", delta="No Data", delta_color="off")
        
    with kpi3:
        st.metric(
            label="Volume (Cumulative)", 
            value=f"{latest['total_volume']:,}"
        )
        
    with kpi4:
        vol = latest.get('volatility', 0.0)
        st.metric(
            label="Volatility (Risk)", 
            value=f"{vol:.4f}",
            delta="High Risk" if vol > 1.0 else "Stable",
            delta_color="inverse"
        )

    # --- ROW 2: SESSION CONTEXT (The "Pro" KPIs) ---
    st.markdown("### 📊 Session Context")
    ctx1, ctx2, ctx3, ctx4 = st.columns(4)
    
    with ctx1:
        st.metric("Session High", f"${session_high:.2f}")
        
    with ctx2:
        st.metric("Session Low", f"${session_low:.2f}")
        
    with ctx3:
        # Spread (High - Low of the current window)
        spread = latest['max_price'] - latest['min_price']
        st.metric("Window Spread", f"${spread:.2f}")
        
    with ctx4:
        # Simple Trend Logic (Price vs 5-window Moving Average)
        ma_5 = df_sorted['avg_price'].head(5).mean()
        trend = "Bullish 🟢" if latest['avg_price'] > ma_5 else "Bearish 🔴"
        st.metric("Short Term Trend", trend)
    
    st.divider()

def render_charts(df_symbol):
    """Render Main Charts with Professional Spacing and Labels"""
    
    # 1. Prepare Data
    df_symbol = df_symbol.sort_values("window_end")
    
    # Calculate Volume Delta (Fix for Cumulative Volume issue)
    df_symbol['volume_delta'] = df_symbol['total_volume'].diff().fillna(0)
    df_symbol['volume_delta'] = df_symbol['volume_delta'].apply(lambda x: x if x > 0 else 0)

    # 2. Create Subplots with SPACING
    fig = make_subplots(
        rows=2, cols=1, 
        shared_xaxes=True, 
        vertical_spacing=0.15, # Space between charts
        row_heights=[0.7, 0.3],
        subplot_titles=(f"{df_symbol['symbol'].iloc[0]} Price Action", "Trading Volume (Per Window)")
    )

    # Price Candlestick (Top Row)
    fig.add_trace(go.Candlestick(
        x=df_symbol['window_end'],
        open=df_symbol['avg_price'], 
        high=df_symbol['max_price'],
        low=df_symbol['min_price'],
        close=df_symbol['avg_price'],
        name="Price"
    ), row=1, col=1)

    # Volume Bars (Bottom Row)
    fig.add_trace(go.Bar(
        x=df_symbol['window_end'], 
        y=df_symbol['volume_delta'],
        name="Volume",
        marker_color='rgba(100, 150, 250, 0.6)'
    ), row=2, col=1)

    # --- PRO STYLING ---
    fig.update_layout(
        template="plotly_dark",
        height=700,
        showlegend=False,
        xaxis_rangeslider_visible=False, # Remove the weird bottom bar
        margin=dict(l=20, r=20, t=40, b=20)
    )
    
    # Add Axis Labels
    fig.update_yaxes(title_text="Price ($)", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)
    fig.update_xaxes(title_text="Time (UTC)", row=2, col=1)
    
    st.plotly_chart(fig, use_container_width=True)

# --- Main App Loop ---
def main():
    st.title("⚡ Real-Time Stock Insight - Lakehouse Dashboard")
    
    status_container = st.empty()
    content_container = st.container()
    
    st.sidebar.header("Configuration")
    auto_refresh = st.sidebar.checkbox("Auto-Refresh", value=True)
    
    df = load_data()
    
    if df is None or df.empty:
        status_container.info("⏳ Connecting to S3 Data Lake... Waiting for Spark to write files.")
        time.sleep(REFRESH_SECONDS)
        if auto_refresh:
            st.rerun()
        return

    # Convert timestamps
    if 'window_end' in df.columns:
        df['window_end'] = pd.to_datetime(df['window_end'])

    # Get available symbols
    symbols = df['symbol'].unique()
    selected_symbol = st.sidebar.selectbox("Select Symbol", symbols)
    
    # Filter
    df_symbol = df[df['symbol'] == selected_symbol].sort_values("window_end")
    
    # Load Predictions (if available)
    prediction = load_predictions(selected_symbol)
    
    with content_container:
        if not df_symbol.empty:
            st.subheader(f"Analysis for {selected_symbol}")
            render_kpis(df_symbol, prediction)
            render_charts(df_symbol)
        else:
            st.info(f"No data for {selected_symbol}")

    if auto_refresh:
        time.sleep(REFRESH_SECONDS)
        st.rerun()

if __name__ == "__main__":
    main()