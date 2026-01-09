"""
🚀 Real-Time Stock Insight - Dashboard UI
Main Streamlit Application with Multi-Page Interface
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
import sys
import threading
import time

# Page configuration
st.set_page_config(
    page_title="finance lake ",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
#MainMenu {visibility: hidden;}
header {visibility: hidden;}
footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .stButton>button {
        width: 100%;
        background-color: #1f77b4;
        color: white;
        font-weight: bold;
    }
    .status-running {
        color: #00cc00;
        font-weight: bold;
    }
    .status-stopped {
        color: #ff4444;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'pipeline_running' not in st.session_state:
    st.session_state.pipeline_running = False
if 'pipeline_thread' not in st.session_state:
    st.session_state.pipeline_thread = None
if 'last_update' not in st.session_state:
    st.session_state.last_update = None

def main():
    """Main dashboard home page"""
    
    # Header
    st.markdown('<h1 class="main-header">Finance Lake</h1>', 
                unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Introduction
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        ### 🎯 Welcome to Finance Lake
        
        This interactive dashboard provides real-time analysis of stock market data using **Apache Spark**.
        
        **Features:**
        - 🌊 **Real-Time Monitoring**: Live streaming data with windowed aggregations
        - 📊 **SQL Analysis**: Interactive SQL queries with caching performance comparison
        - 🤖 **ML Models**: Predictive models (Logistic Regression & Random Forest)
        - 📈 **Visualizations**: Advanced charts with technical indicators (RSI)
        """)
        
    with col2:
        st.markdown("### 📊 Quick Stats")
        
        # Check if data exists
        if os.path.exists("./output/stock_data"):
            try:
                files = [f for f in os.listdir("./output/stock_data") 
                        if f.endswith('.parquet')]
                st.metric("Data Files", len(files))
            except:
                st.metric("Data Files", "0")
        else:
            st.metric("Data Files", "0")
        
        if os.path.exists("./stream_data"):
            try:
                files = [f for f in os.listdir("./stream_data") 
                        if f.endswith('.json')]
                st.metric("Stream Files", len(files))
            except:
                st.metric("Stream Files", "0")
        else:
            st.metric("Stream Files", "0")
    
    st.markdown("---")
    
    # Pipeline Control Section
    st.markdown("### 🎮 Pipeline Control")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("#### Status")
        if st.session_state.pipeline_running:
            st.markdown('<p class="status-running">🟢 RUNNING</p>', 
                       unsafe_allow_html=True)
        else:
            st.markdown('<p class="status-stopped">🔴 STOPPED</p>', 
                       unsafe_allow_html=True)
    
    with col2:
        st.markdown("#### Actions")
        if not st.session_state.pipeline_running:
            if st.button("▶️ Start Pipeline", key="start"):
                start_pipeline()
        else:
            if st.button("⏹️ Stop Pipeline", key="stop"):
                stop_pipeline()
    
    with col3:
        st.markdown("#### Last Update")
        if st.session_state.last_update:
            st.info(st.session_state.last_update.strftime("%H:%M:%S"))
        else:
            st.info("Not running")
    
    st.markdown("---")
    
    # Navigation Guide
    st.markdown("### 🧭 Navigation Guide")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("""
        #### 🌊 Real-Time Monitoring
        - Live streaming data
        - Windowed aggregations
        - Price & volume metrics
        - Volatility tracking
        """)
    
    with col2:
        st.markdown("""
        #### 📊 SQL Analysis
        - Interactive queries
        - Cache performance
        - Execution plans
        - Top performers
        """)
    
    with col3:
        st.markdown("""
        #### 🤖 ML Models
        - Model comparison
        - Performance metrics
        - Predictions
        - Feature importance
        """)
    
    with col4:
        st.markdown("""
        #### 📈 Visualizations
        - Price trends
        - Volume analysis
        - RSI indicator
        - Correlation matrix
        """)
    
    st.markdown("---")
    
    # System Information
    st.markdown("### ℹ️ System Information")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.info(f"**Python Version:** {sys.version.split()[0]}")
    
    with col2:
        try:
            from pyspark import __version__ as spark_version
            st.info(f"**Spark Version:** {spark_version}")
        except:
            st.info("**Spark Version:** Not loaded")
    
    with col3:
        st.info(f"**Streamlit Version:** {st.__version__}")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; padding: 1rem;'>
        <p>📚 Developed by KHODDER Soukaina & OURRAIS Soad</p>
        <p>Finance Lakev1.0</p>
    </div>
    """, unsafe_allow_html=True)

def start_pipeline():
    """Start the Spark pipeline in background thread"""
    try:
        st.session_state.pipeline_running = True
        st.session_state.last_update = datetime.now()
        
        # Import and start pipeline in thread
        def run_pipeline():
            try:
                from main import RealTimeStockInsight
                app = RealTimeStockInsight()
                app.run_complete_pipeline(duration=300)  # 5 minutes
            except Exception as e:
                st.error(f"Pipeline error: {e}")
                st.session_state.pipeline_running = False
        
        thread = threading.Thread(target=run_pipeline, daemon=True)
        thread.start()
        st.session_state.pipeline_thread = thread
        
        st.success("✅ Pipeline started successfully!")
        st.balloons()
        
    except Exception as e:
        st.error(f"❌ Error starting pipeline: {e}")
        st.session_state.pipeline_running = False

def stop_pipeline():
    """Stop the running pipeline"""
    try:
        st.session_state.pipeline_running = False
        st.warning("⏹️ Pipeline stopping... (may take a few seconds)")
        time.sleep(2)
        st.success("✅ Pipeline stopped")
        
    except Exception as e:
        st.error(f"❌ Error stopping pipeline: {e}")

if __name__ == "__main__":
    main()

