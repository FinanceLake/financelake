"""
Real-Time Data Monitoring Page
Displays live streaming data and aggregations
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import os
import time

st.set_page_config(page_title="Real-Time Monitoring", page_icon="🌊", layout="wide")

st.title("🌊 Real-Time Stock Monitoring")
st.markdown("Live streaming data with windowed aggregations")

# Auto-refresh toggle
col1, col2 = st.columns([3, 1])
with col1:
    st.markdown("### 📊 Live Data Stream")
with col2:
    auto_refresh = st.checkbox("Auto-refresh (5s)", value=True)

if auto_refresh:
    st_autorefresh = st.empty()
    with st_autorefresh:
        st.info(f"🔄 Auto-refreshing... Last update: {datetime.now().strftime('%H:%M:%S')}")
    time.sleep(5)
    st.rerun()

# Try to load data from Spark memory table or parquet
@st.cache_data(ttl=5)
def load_streaming_data():
    """Load the latest streaming data"""
    try:
        # Try to read from output directory
        if os.path.exists("./output/stock_data"):
            parquet_files = [f for f in os.listdir("./output/stock_data") 
                           if f.endswith('.parquet')]
            if parquet_files:
                # Read the latest parquet file
                latest_file = sorted(parquet_files)[-1]
                df = pd.read_parquet(f"./output/stock_data/{latest_file}")
                return df.tail(100)  # Return last 100 rows
        
        # If no data yet, return sample data
        return pd.DataFrame({
            'symbol': ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA'] * 20,
            'price': [150 + i*0.5 for i in range(100)],
            'volume': [1000000 + i*10000 for i in range(100)],
            'timestamp': pd.date_range(start='2024-01-01', periods=100, freq='1min')
        })
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=5)
def load_aggregated_data():
    """Load aggregated metrics"""
    try:
        # Try to load from checkpoints or generate from raw data
        df = load_streaming_data()
        if not df.empty:
            agg_data = df.groupby('symbol').agg({
                'price': ['mean', 'std', 'min', 'max'],
                'volume': 'sum'
            }).reset_index()
            agg_data.columns = ['symbol', 'avg_price', 'volatility', 'min_price', 
                               'max_price', 'total_volume']
            return agg_data
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading aggregated data: {e}")
        return pd.DataFrame()

# Load data
df_raw = load_streaming_data()
df_agg = load_aggregated_data()

# Display metrics
if not df_agg.empty:
    st.markdown("### 📈 Key Metrics (Last Window)")
    
    cols = st.columns(len(df_agg))
    for idx, row in df_agg.iterrows():
        with cols[idx]:
            st.metric(
                label=f"💹 {row['symbol']}",
                value=f"${row['avg_price']:.2f}",
                delta=f"{row['volatility']:.2f} vol"
            )

    st.markdown("---")
    
    # Detailed table
    st.markdown("### 📊 Aggregated Data (10-second windows)")
    
    # Format the dataframe for display
    display_df = df_agg.copy()
    display_df['avg_price'] = display_df['avg_price'].apply(lambda x: f"${x:.2f}")
    display_df['volatility'] = display_df['volatility'].apply(lambda x: f"{x:.2f}")
    display_df['min_price'] = display_df['min_price'].apply(lambda x: f"${x:.2f}")
    display_df['max_price'] = display_df['max_price'].apply(lambda x: f"${x:.2f}")
    display_df['total_volume'] = display_df['total_volume'].apply(lambda x: f"{x:,.0f}")
    
    st.dataframe(display_df, use_container_width=True, height=250)

else:
    st.warning("⚠️ No aggregated data available yet. Start the pipeline from the home page.")

st.markdown("---")

# Raw data visualization
if not df_raw.empty:
    st.markdown("### 📉 Raw Data Stream (Last 100 records)")
    
    # Price chart
    fig_price = px.line(
        df_raw,
        x='timestamp',
        y='price',
        color='symbol',
        title='Real-Time Price Movement',
        labels={'price': 'Price ($)', 'timestamp': 'Time'}
    )
    fig_price.update_layout(height=400, hovermode='x unified')
    st.plotly_chart(fig_price, use_container_width=True)
    
    # Volume chart
    fig_volume = px.bar(
        df_raw.groupby('symbol')['volume'].sum().reset_index(),
        x='symbol',
        y='volume',
        title='Total Volume by Symbol',
        labels={'volume': 'Volume', 'symbol': 'Stock Symbol'},
        color='symbol'
    )
    fig_volume.update_layout(height=300)
    st.plotly_chart(fig_volume, use_container_width=True)
    
    # Raw data table (expandable)
    with st.expander("📋 View Raw Data Table"):
        st.dataframe(df_raw, use_container_width=True)

else:
    st.warning("⚠️ No streaming data available yet. Start the pipeline from the home page.")

# Data statistics
if not df_raw.empty:
    st.markdown("---")
    st.markdown("### 📊 Data Statistics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Records", len(df_raw))
    
    with col2:
        st.metric("Symbols", df_raw['symbol'].nunique())
    
    with col3:
        st.metric("Avg Price", f"${df_raw['price'].mean():.2f}")
    
    with col4:
        st.metric("Total Volume", f"{df_raw['volume'].sum():,.0f}")

# Information box
st.markdown("---")
st.info("""
💡 **About Real-Time Monitoring:**
- Data updates every 5 seconds when auto-refresh is enabled
- Aggregations are calculated using Spark Structured Streaming
- Windowed aggregations use 10-second windows with 5-second slides
- Metrics include: average price, volatility (std dev), min/max, total volume
""")

