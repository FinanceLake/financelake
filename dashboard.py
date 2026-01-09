#!/usr/bin/env python3
"""
Streamlit Dashboard for FinanceLake Spark Results
Updated for Streamlit 2026 API standards
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from deltalake import DeltaTable
import time
import os
from datetime import datetime

# Paths (must match your Spark script)
gold_metrics_path = "data/gold/metrics"
gold_summary_path = "data/gold/summary"
silver_stocks_path = "data/silver/stocks"

st.set_page_config(page_title="FinanceLake Dashboard", layout="wide")

# Dashboard Header
st.title("🚀 FinanceLake Real-Time Monitoring Dashboard")
st.markdown("""
Monitoring the **Gold layer** results from your Spark + Delta Lake pipeline  
(Data source: Alpha Vantage)
""")

def load_delta_table(path: str) -> pd.DataFrame:
    """Safely load a Delta table into a pandas DataFrame."""
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        dt = DeltaTable(path)
        return dt.to_pandas()
    except Exception:
        # Returns empty if table is being initialized by Spark
        return pd.DataFrame()

# Load data
metrics_df = load_delta_table(gold_metrics_path)
summary_df = load_delta_table(gold_summary_path)
silver_df = load_delta_table(silver_stocks_path)

# Layout
col1, col2 = st.columns(2)

# Left column: Daily Metrics
with col1:
    st.subheader("📊 Daily Stock Metrics (Gold Layer)")
    if not metrics_df.empty:
        # Sort latest first
        display_metrics = metrics_df.sort_values(['symbol', 'date'], ascending=[True, False])
        st.dataframe(display_metrics, width="stretch")

        # Line chart: Close price per symbol
        fig_close = px.line(
            metrics_df,
            x='date',
            y='close_price',
            color='symbol',
            title="Close Prices Over Time",
            markers=True
        )
        # FIX: Added unique key to avoid DuplicateElementId
        st.plotly_chart(fig_close, width="stretch", key="chart_stock_prices")
    else:
        st.info("No daily metrics available yet. Run the Spark batch job first.")

# Right column: Market Summary
with col2:
    st.subheader("🌍 Market Summary (Gold Layer)")
    if not summary_df.empty:
        display_summary = summary_df.sort_values('date', ascending=False)
        st.dataframe(display_summary, width="stretch")

        # Average market price trend
        fig_market = px.line(
            summary_df,
            x='date',
            y='avg_market_price',
            title="Average Market Price Trend",
            markers=True
        )
        # FIX: Added unique key to avoid DuplicateElementId
        st.plotly_chart(fig_market, width="stretch", key="chart_market_avg")

        # Key metrics from latest day
        latest = summary_df.sort_values('date').iloc[-1]
        m1, m2, m3, m4 = st.columns(4)
        
        m1.metric("Latest Date", str(latest['date']))
        m2.metric("Avg Market Price", f"${latest['avg_market_price']:.2f}")
        m3.metric("Total Volume", f"{int(latest['total_market_volume']):,}")
        m4.metric("Number of Symbols", int(latest['num_symbols']))
    else:
        st.info("No market summary available yet.")

# ML Predictions section
st.divider()
st.subheader("🤖 MLlib Predictions (Linear Regression)")
if not silver_df.empty:
    latest_prices = silver_df.sort_values(['symbol', 'date']).groupby('symbol').tail(1)
    st.dataframe(latest_prices[['symbol', 'date', 'close']], width="stretch")
    st.success("Data is flowing correctly. Check your Spark terminal for real-time Prediction outputs.")
else:
    st.info("Waiting for data in the Silver layer...")

# Footer & Auto-Refresh Logic
st.caption(
    f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
    f"Auto-refreshing every 30 seconds..."
)

# Replace the 'while True' loop with st.rerun()
time.sleep(30)
st.rerun()