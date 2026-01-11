import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================================
# CONFIGURATION
# ============================================================================
st.set_page_config(
    page_title="Stock Data Lake - Real-time Analytics",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for professional look
st.markdown("""
<style>
    /* Main background */
    .stApp {
        background-color: #0f172a;
    }
    
    /* Hide default Streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display:none;}
    
    /* Header styling */
    .main-header {
        background: linear-gradient(90deg, #1e40af 0%, #3b82f6 100%);
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }
    
    /* Metric cards */
    .metric-card {
        background: rgba(30, 41, 59, 0.7);
        border-radius: 10px;
        padding: 1.5rem;
        border: 1px solid rgba(148, 163, 184, 0.1);
        backdrop-filter: blur(10px);
    }
    
    /* Status indicators */
    .status-indicator {
        display: inline-block;
        width: 10px;
        height: 10px;
        border-radius: 50%;
        margin-right: 8px;
    }
    
    .status-live {
        background-color: #10b981;
        box-shadow: 0 0 10px #10b981;
    }
    
    .status-processing {
        background-color: #f59e0b;
        box-shadow: 0 0 10px #f59e0b;
    }
    
    /* Pipeline stage cards */
    .pipeline-stage {
        background: rgba(15, 23, 42, 0.8);
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
        border-left: 4px solid;
    }
    
    .stage-bronze {
        border-left-color: #f59e0b;
    }
    
    .stage-silver {
        border-left-color: #3b82f6;
    }
    
    .stage-gold {
        border-left-color: #10b981;
    }
    
    /* Chart containers */
    .chart-container {
        background: rgba(30, 41, 59, 0.7);
        border-radius: 12px;
        padding: 1.5rem;
        border: 1px solid rgba(148, 163, 184, 0.1);
        margin: 1rem 0;
    }
    
    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 1rem;
        background-color: rgba(15, 23, 42, 0.8);
        padding: 0.5rem;
        border-radius: 10px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background-color: transparent;
        border-radius: 8px;
        padding: 0.75rem 1.5rem;
        font-weight: 600;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: rgba(59, 130, 246, 0.2);
        color: #60a5fa;
        border: 1px solid rgba(59, 130, 246, 0.3);
    }
    
    /* Real-time badge */
    .real-time-badge {
        animation: pulse 2s infinite;
    }
    
    @keyframes pulse {
        0% { opacity: 1; }
        50% { opacity: 0.5; }
        100% { opacity: 1; }
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# INITIALIZE SPARK
# ============================================================================
@st.cache_resource
def init_spark():
    try:
        spark = SparkSession.builder \
            .appName("StockAnalytics") \
            .master("local[2]") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception as e:
        st.sidebar.error(f"Spark initialization failed: {str(e)}")
        return None

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================
def safe_convert_to_datetime(value):
    """Safely convert value to datetime"""
    if value is None:
        return None
    try:
        if isinstance(value, datetime):
            return value
        elif isinstance(value, str):
            return pd.to_datetime(value, errors='coerce')
        else:
            return None
    except:
        return None

def format_datetime(dt_obj, format_str="%H:%M:%S"):
    """Safely format datetime to string"""
    if dt_obj is None:
        return "N/A"
    try:
        if isinstance(dt_obj, datetime):
            return dt_obj.strftime(format_str)
        elif isinstance(dt_obj, str):
            dt = pd.to_datetime(dt_obj, errors='coerce')
            if pd.isna(dt):
                return "N/A"
            return dt.strftime(format_str)
        return str(dt_obj)
    except:
        return "N/A"

# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================
def load_delta_data(table_path):
    """Load data from Delta table"""
    try:
        spark = init_spark()
        if spark and os.path.exists(table_path):
            df = spark.read.format("delta").load(table_path)
            if df.count() > 0:
                # Convert timestamp to string before pandas conversion
                df = df.withColumn("timestamp", F.col("timestamp").cast("string"))
                pdf = df.toPandas()
                if 'timestamp' in pdf.columns:
                    pdf['timestamp'] = pd.to_datetime(pdf['timestamp'], errors='coerce')
                return pdf
    except Exception as e:
        print(f"Data loading error for {table_path}: {str(e)}")
    return pd.DataFrame()

@st.cache_data(ttl=5)  # Cache for 5 seconds for real-time updates
def get_live_data(symbol=None, limit=1000):
    """Get latest data with cache for performance"""
    data = load_delta_data("/app/storage/silver_stock")
    if data.empty:
        return pd.DataFrame()
    
    if symbol and symbol != "All":
        data = data[data['symbol'] == symbol]
    
    return data.sort_values('timestamp').tail(limit)

def get_pipeline_stats():
    """Get pipeline statistics - FIXED VERSION"""
    stats = {
        'bronze': {'count': 0, 'last_update': None},
        'silver': {'count': 0, 'last_update': None},
        'gold': {'count': 0, 'last_update': None}
    }
    
    try:
        spark = init_spark()
        if spark is None:
            return stats
        
        # Bronze stats
        bronze_path = "/app/storage/bronze_stock"
        if os.path.exists(bronze_path):
            try:
                bronze_df = spark.read.format("delta").load(bronze_path)
                count = bronze_df.count()
                stats['bronze']['count'] = count
                
                if count > 0 and 'timestamp' in bronze_df.columns:
                    max_time_result = bronze_df.agg(F.max("timestamp").alias("max_time")).collect()[0]
                    if max_time_result is not None:
                        max_time = max_time_result["max_time"]
                        stats['bronze']['last_update'] = max_time
            except Exception as e:
                print(f"Error loading bronze: {e}")
        
        # Silver stats
        silver_path = "/app/storage/silver_stock"
        if os.path.exists(silver_path):
            try:
                silver_df = spark.read.format("delta").load(silver_path)
                count = silver_df.count()
                stats['silver']['count'] = count
                
                if count > 0 and 'timestamp' in silver_df.columns:
                    max_time_result = silver_df.agg(F.max("timestamp").alias("max_time")).collect()[0]
                    if max_time_result is not None:
                        max_time = max_time_result["max_time"]
                        stats['silver']['last_update'] = max_time
            except Exception as e:
                print(f"Error loading silver: {e}")
        
        # Gold stats - use current time if data exists
        gold_path = "/app/storage/gold_stock"
        if os.path.exists(gold_path):
            try:
                gold_df = spark.read.format("delta").load(gold_path)
                count = gold_df.count()
                stats['gold']['count'] = count
                
                if count > 0:
                    stats['gold']['last_update'] = datetime.now()
            except Exception as e:
                print(f"Error loading gold: {e}")
                
    except Exception as e:
        print(f"Pipeline stats error: {e}")
    
    return stats

# ============================================================================
# VISUALIZATION FUNCTIONS
# ============================================================================
def create_realtime_price_chart(df, symbol):
    """Create real-time price chart with technical indicators"""
    if df.empty or len(df) < 2:
        return None
    
    fig = make_subplots(
        rows=4, cols=1,
        row_heights=[0.4, 0.2, 0.2, 0.2],
        vertical_spacing=0.05,
        subplot_titles=(
            f'📊 {symbol} - Price & Volume',
            '📈 RSI Indicator',
            '📉 MACD Signal',
            '⚡ Price Change'
        ),
        shared_xaxes=True
    )
    
    # 1. Price with moving averages
    fig.add_trace(
        go.Scatter(
            x=df['timestamp'],
            y=df['price'],
            name='Price',
            line=dict(color='#3b82f6', width=2),
            hovertemplate='<b>Price</b>: $%{y:.2f}<br>Time: %{x}<extra></extra>'
        ),
        row=1, col=1
    )
    
    # Add moving average (7-period)
    if len(df) >= 7:
        ma7 = df['price'].rolling(window=7).mean()
        fig.add_trace(
            go.Scatter(
                x=df['timestamp'],
                y=ma7,
                name='MA7',
                line=dict(color='#f59e0b', width=1, dash='dash'),
                hovertemplate='<b>MA7</b>: $%{y:.2f}<extra></extra>'
            ),
            row=1, col=1
        )
    
    # Volume bars
    if 'volume' in df.columns:
        colors = ['#10b981' if df['price'].iloc[i] >= df['price'].iloc[i-1] 
                 else '#ef4444' for i in range(len(df))]
        colors[0] = '#6b7280'  # First bar neutral
        
        fig.add_trace(
            go.Bar(
                x=df['timestamp'],
                y=df['volume'],
                name='Volume',
                marker_color=colors,
                opacity=0.7,
                hovertemplate='<b>Volume</b>: %{y:,}<extra></extra>',
                yaxis='y2'
            ),
            row=1, col=1
        )
    
    # 2. RSI Indicator
    if 'rsi' in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df['timestamp'],
                y=df['rsi'],
                name='RSI',
                line=dict(color='#8b5cf6', width=2),
                hovertemplate='<b>RSI</b>: %{y:.1f}<extra></extra>'
            ),
            row=2, col=1
        )
        
        # RSI zones
        fig.add_hrect(y0=70, y1=100, fillcolor="rgba(239, 68, 68, 0.1)", 
                     line_width=0, row=2, col=1)
        fig.add_hrect(y0=0, y1=30, fillcolor="rgba(34, 197, 94, 0.1)", 
                     line_width=0, row=2, col=1)
        fig.add_hline(y=70, line_dash="dash", line_color="rgba(239, 68, 68, 0.5)", 
                     line_width=1, row=2, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="rgba(34, 197, 94, 0.5)", 
                     line_width=1, row=2, col=1)
        fig.add_hline(y=50, line_dash="dot", line_color="rgba(148, 163, 184, 0.5)", 
                     line_width=0.5, row=2, col=1)
    
    # 3. MACD
    if 'macd' in df.columns:
        colors_macd = ['#10b981' if val >= 0 else '#ef4444' for val in df['macd']]
        
        fig.add_trace(
            go.Bar(
                x=df['timestamp'],
                y=df['macd'],
                name='MACD',
                marker_color=colors_macd,
                opacity=0.7,
                hovertemplate='<b>MACD</b>: %{y:.3f}<extra></extra>'
            ),
            row=3, col=1
        )
        fig.add_hline(y=0, line_color="rgba(148, 163, 184, 0.5)", 
                     line_width=1, row=3, col=1)
    
    # 4. Price Change
    if 'price_change' in df.columns:
        colors_change = ['#10b981' if val >= 0 else '#ef4444' for val in df['price_change']]
        
        fig.add_trace(
            go.Bar(
                x=df['timestamp'],
                y=df['price_change'],
                name='Price Change',
                marker_color=colors_change,
                hovertemplate='<b>Change</b>: $%{y:.2f}<extra></extra>'
            ),
            row=4, col=1
        )
        fig.add_hline(y=0, line_color="rgba(148, 163, 184, 0.5)", 
                     line_width=1, row=4, col=1)
    
    # Update layout
    fig.update_layout(
        height=900,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        hovermode='x unified',
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#cbd5e1', size=12),
        margin=dict(l=0, r=0, t=50, b=0),
        xaxis=dict(
            showgrid=True,
            gridcolor='rgba(148, 163, 184, 0.1)',
            showline=True,
            linecolor='rgba(148, 163, 184, 0.2)'
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='rgba(148, 163, 184, 0.1)',
            showline=True,
            linecolor='rgba(148, 163, 184, 0.2)',
            title=dict(text='Price ($)', font=dict(color='#94a3b8'))
        ),
        yaxis2=dict(
            showgrid=False,
            overlaying='y',
            side='right',
            title=dict(text='Volume', font=dict(color='#94a3b8'))
        )
    )
    
    # Update subplot titles
    fig.update_annotations(font=dict(size=14, color='#f1f5f9', family="Arial"))
    
    # Update axes for all subplots
    for i in range(1, 5):
        fig.update_xaxes(
            showgrid=True,
            gridcolor='rgba(148, 163, 184, 0.1)',
            row=i, col=1
        )
        fig.update_yaxes(
            showgrid=True,
            gridcolor='rgba(148, 163, 184, 0.1)',
            row=i, col=1
        )
    
    return fig

def create_pipeline_visualization(stats):
    """Create visualization of the data pipeline"""
    fig = go.Figure()
    
    # Pipeline stages as nodes
    stages = ['Bronze', 'Silver', 'Gold']
    x_pos = [0, 1, 2]
    counts = [stats['bronze']['count'], stats['silver']['count'], stats['gold']['count']]
    colors = ['#f59e0b', '#3b82f6', '#10b981']
    
    # Add nodes
    for i, (stage, count, color) in enumerate(zip(stages, counts, colors)):
        fig.add_trace(go.Scatter(
            x=[x_pos[i]],
            y=[0],
            mode='markers+text',
            marker=dict(size=80, color=color, line=dict(width=2, color='white')),
            text=[f"<b>{stage}</b><br>{count:,}"],
            textposition="middle center",
            textfont=dict(size=14, color='white'),
            name=stage
        ))
    
    # Add connections
    for i in range(len(stages)-1):
        fig.add_trace(go.Scatter(
            x=[x_pos[i], x_pos[i+1]],
            y=[0, 0],
            mode='lines',
            line=dict(color='#64748b', width=4, dash='dash'),
            showlegend=False
        ))
    
    # Add arrows
    for i in range(len(stages)-1):
        fig.add_annotation(
            x=x_pos[i+1]-0.1,
            y=0,
            ax=x_pos[i]+0.1,
            ay=0,
            xref="x",
            yref="y",
            axref="x",
            ayref="y",
            showarrow=True,
            arrowhead=2,
            arrowsize=1,
            arrowwidth=2,
            arrowcolor="#64748b"
        )
    
    fig.update_layout(
        title="Data Pipeline Flow (Bronze → Silver → Gold)",
        height=300,
        showlegend=False,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#cbd5e1'),
        xaxis=dict(
            showgrid=False,
            zeroline=False,
            showticklabels=False,
            range=[-0.5, 2.5]
        ),
        yaxis=dict(
            showgrid=False,
            zeroline=False,
            showticklabels=False,
            range=[-1, 1]
        ),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    
    return fig

def create_stock_performance_grid(data):
    """Create performance grid for all stocks"""
    if data.empty or len(data) < 2:
        return None
    
    # Group by symbol and calculate metrics
    grouped = data.groupby('symbol').agg({
        'price': ['last', 'mean', 'std'],
        'volume': 'sum',
        'price_direction': 'mean',
        'volatility': 'mean'
    }).round(2)
    
    # Flatten column names
    grouped.columns = ['_'.join(col).strip() for col in grouped.columns.values]
    
    # Calculate additional metrics
    latest_prices = data.groupby('symbol').last()['price']
    prev_prices = data.groupby('symbol').apply(lambda x: x.iloc[-2]['price'] if len(x) > 1 else x.iloc[0]['price'])
    price_change_pct = ((latest_prices - prev_prices) / prev_prices * 100).round(2)
    
    # Create summary dataframe
    summary = pd.DataFrame({
        'Symbol': grouped.index,
        'Price': latest_prices.values,
        'Change %': price_change_pct.values,
        'Avg Price': grouped['price_mean'].values,
        'Total Volume': grouped['volume_sum'].values,
        'Volatility': grouped['volatility_mean'].values,
        'Upward %': (grouped['price_direction_mean'] * 100).round(1)
    })
    
    return summary

def create_ml_performance_gauge(accuracy):
    """Create gauge for ML model performance"""
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=accuracy * 100,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': "ML Model Accuracy", 'font': {'size': 16, 'color': '#cbd5e1'}},
        number={'suffix': '%', 'font': {'size': 40, 'color': '#f1f5f9'}},
        gauge={
            'axis': {'range': [None, 100], 'tickwidth': 1, 'tickcolor': "#94a3b8"},
            'bar': {'color': "#3b82f6"},
            'bgcolor': "rgba(30, 41, 59, 0.8)",
            'borderwidth': 2,
            'bordercolor': "rgba(148, 163, 184, 0.3)",
            'steps': [
                {'range': [0, 50], 'color': 'rgba(239, 68, 68, 0.3)'},
                {'range': [50, 70], 'color': 'rgba(245, 158, 11, 0.3)'},
                {'range': [70, 100], 'color': 'rgba(34, 197, 94, 0.3)'}
            ],
            'threshold': {
                'line': {'color': "white", 'width': 3},
                'thickness': 0.75,
                'value': accuracy * 100
            }
        }
    ))
    
    fig.update_layout(
        height=300,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font={'color': "#cbd5e1", 'family': "Arial"},
        margin=dict(l=20, r=20, t=50, b=20)
    )
    
    return fig

# ============================================================================
# SIDEBAR - CORRIGÉ
# ============================================================================
def create_sidebar():
    """Create the sidebar with filters and info - FIXED VERSION"""
    with st.sidebar:
        st.markdown("""
        <div class="main-header">
            <h2 style="color: white; margin: 0;">📊 Stock Data Lake</h2>
            <p style="color: #cbd5e1; margin: 0.5rem 0 0 0;">Real-time Financial Analytics</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Real-time status
        st.markdown("""
        <div style="display: flex; align-items: center; margin: 1rem 0;">
            <span class="status-indicator status-live"></span>
            <span style="color: #cbd5e1; font-weight: 600;">LIVE STREAMING</span>
        </div>
        """, unsafe_allow_html=True)
        
        # Last update time
        current_time = datetime.now().strftime("%H:%M:%S")
        st.markdown(f"""
        <div style="color: #94a3b8; font-size: 0.9rem; margin-bottom: 1rem;">
            Dashboard updated: <span style="color: #60a5fa; font-weight: 600;">{current_time}</span>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Filters
        st.markdown("### 🎯 Filters")
        
        # Symbol filter - version simplifiée
        symbols = ["All", "AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
        selected_symbol = st.selectbox(
            "Select Symbol",
            symbols,
            index=0,
            help="Filter by stock symbol"
        )
        
        # Time range filter
        time_range = st.selectbox(
            "Time Range",
            ["Last 1 hour", "Last 6 hours", "Last 12 hours", "Last 24 hours", "Last 48 hours"],
            index=2,
            help="Select time range for analysis"
        )
        
        # Data points limit
        data_limit = st.slider(
            "Max Data Points",
            min_value=100,
            max_value=5000,
            value=1000,
            step=100,
            help="Limit number of data points for performance"
        )
        
        st.markdown("---")
        
        # Pipeline info - version corrigée
        st.markdown("### 🏗️ Pipeline Status")
        
        try:
            stats = get_pipeline_stats()
        except Exception as e:
            stats = {
                'bronze': {'count': 0, 'last_update': None},
                'silver': {'count': 0, 'last_update': None},
                'gold': {'count': 0, 'last_update': None}
            }
        
        for stage, color_class in [("Bronze", "stage-bronze"), ("Silver", "stage-silver"), ("Gold", "stage-gold")]:
            count = stats[stage.lower()]['count']
            last_update = stats[stage.lower()]['last_update']
            
            # Utiliser la fonction safe pour formater la date
            update_str = format_datetime(last_update, "%H:%M:%S")
            
            st.markdown(f"""
            <div class="pipeline-stage {color_class}">
                <div style="font-weight: 600; color: #f1f5f9;">{stage} Layer</div>
                <div style="display: flex; justify-content: space-between; margin-top: 0.5rem;">
                    <span style="color: #94a3b8;">Records:</span>
                    <span style="color: #60a5fa; font-weight: 600;">{count:,}</span>
                </div>
                <div style="display: flex; justify-content: space-between;">
                    <span style="color: #94a3b8;">Updated:</span>
                    <span style="color: #94a3b8; font-size: 0.9rem;">{update_str}</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Refresh control
        auto_refresh = st.checkbox("🔄 Enable Auto-refresh", value=True, 
                                  help="Automatically refresh data every 5 seconds")
        
        if st.button("🔄 Refresh Now", use_container_width=True, type="primary"):
            st.cache_data.clear()
            st.rerun()
        
        return selected_symbol, time_range, data_limit, auto_refresh

# ============================================================================
# MAIN APP
# ============================================================================
def main():
    # Initialize session state for auto-refresh
    if 'last_refresh' not in st.session_state:
        st.session_state.last_refresh = datetime.now()
    
    # Create sidebar and get filters
    selected_symbol, time_range, data_limit, auto_refresh = create_sidebar()
    
    # Main content area
    st.markdown("""
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
        <h1 style="color: #f1f5f9; margin: 0;">Real-time Stock Analytics Dashboard</h1>
        <div style="display: flex; align-items: center;">
            <span class="status-indicator status-live real-time-badge"></span>
            <span style="color: #94a3b8; margin-left: 0.5rem; font-size: 0.9rem;">Streaming Active</span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Load data
    with st.spinner("⏳ Loading real-time data..."):
        data = get_live_data(
            symbol=selected_symbol if selected_symbol != "All" else None,
            limit=data_limit
        )
    
    # Display data status
    if data.empty:
        st.warning("⚠️ No data available. Waiting for pipeline to process data...")
        st.info("The data pipeline is starting up. Data will appear shortly.")
        
        # Show pipeline stats even if no data
        st.markdown("### 🏗️ Pipeline Initialization")
        stats = get_pipeline_stats()
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Bronze Layer", f"{stats['bronze']['count']:,}", "Raw Data")
        with col2:
            st.metric("Silver Layer", f"{stats['silver']['count']:,}", "Cleaned Data")
        with col3:
            st.metric("Gold Layer", f"{stats['gold']['count']:,}", "Aggregated KPIs")
        
        return
    
    # Key Metrics
    st.markdown("### 📊 Key Metrics")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        latest_price = data['price'].iloc[-1]
        prev_price = data['price'].iloc[-2] if len(data) > 1 else latest_price
        price_change = ((latest_price - prev_price) / prev_price * 100) if prev_price != 0 else 0
        delta_color = "normal" if price_change >= 0 else "inverse"
        st.metric("Current Price", f"${latest_price:.2f}", f"{price_change:+.2f}%", delta_color)
    
    with col2:
        total_volume = data['volume'].sum() if 'volume' in data.columns else 0
        st.metric("Total Volume", f"{total_volume:,}", "")
    
    with col3:
        avg_volatility = data['volatility'].mean() if 'volatility' in data.columns else 0
        st.metric("Avg Volatility", f"{avg_volatility:.3f}", "")
    
    with col4:
        avg_rsi = data['rsi'].mean() if 'rsi' in data.columns else 0
        rsi_status = "Overbought" if avg_rsi > 70 else "Oversold" if avg_rsi < 30 else "Neutral"
        st.metric("Avg RSI", f"{avg_rsi:.1f}", rsi_status)
    
    with col5:
        upward_percent = (data['price_direction'].mean() * 100) if 'price_direction' in data.columns else 0
        st.metric("Upward Trend", f"{upward_percent:.1f}%", "")
    
    # Main Tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Real-time Charts", "📊 Performance Grid", "🏗️ Pipeline", "ℹ️ Architecture"])
    
    with tab1:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        
        if selected_symbol == "All":
            # Show all symbols in separate charts
            symbols = data['symbol'].unique()[:5]  # Limit to 5 symbols
            for symbol in symbols:
                symbol_data = data[data['symbol'] == symbol]
                if len(symbol_data) > 1:
                    st.subheader(f"📊 {symbol}")
                    fig = create_realtime_price_chart(symbol_data, symbol)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
                    st.markdown("---")
        else:
            # Show detailed chart for selected symbol
            fig = create_realtime_price_chart(data, selected_symbol)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Raw data preview
        with st.expander("📋 View Raw Data (Latest 50 rows)"):
            st.dataframe(
                data.tail(50),
                use_container_width=True,
                height=400,
                column_config={
                    "timestamp": st.column_config.DatetimeColumn("Timestamp"),
                    "price": st.column_config.NumberColumn("Price", format="$%.2f"),
                    "volume": st.column_config.NumberColumn("Volume", format="%d")
                }
            )
    
    with tab2:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.markdown("### 📈 Stock Performance Comparison")
            
            # Create performance grid
            performance_grid = create_stock_performance_grid(data)
            if performance_grid is not None:
                # Style the dataframe
                styled_grid = performance_grid.style.format({
                    'Price': '${:.2f}',
                    'Change %': '{:.2f}%',
                    'Avg Price': '${:.2f}',
                    'Total Volume': '{:,}',
                    'Volatility': '{:.3f}',
                    'Upward %': '{:.1f}%'
                }).apply(lambda x: ['background-color: rgba(34, 197, 94, 0.2)' 
                                  if val > 0 else 'background-color: rgba(239, 68, 68, 0.2)' 
                                  for val in x], subset=['Change %'])
                
                st.dataframe(styled_grid, use_container_width=True, height=400)
            else:
                st.info("Not enough data to show performance comparison")
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.markdown("### 🧠 ML Performance")
            
            # Simulate ML accuracy (in real implementation, load from ML model)
            ml_accuracy = 0.72  # This would come from your ML model
            
            fig = create_ml_performance_gauge(ml_accuracy)
            st.plotly_chart(fig, use_container_width=True)
            
            st.markdown("""
            <div style="color: #94a3b8; font-size: 0.9rem; margin-top: 1rem;">
                <b>Model Details:</b><br>
                • Algorithm: Random Forest<br>
                • Features: RSI, MACD, Volatility<br>
                • Target: Price Direction<br>
                • Training: Every 5 minutes
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        st.markdown("### 🏗️ Data Pipeline Visualization")
        
        stats = get_pipeline_stats()
        fig = create_pipeline_visualization(stats)
        st.plotly_chart(fig, use_container_width=True)
        
        # Pipeline description
        st.markdown("""
        <div style="background: rgba(30, 41, 59, 0.5); padding: 1.5rem; border-radius: 8px; margin-top: 1rem;">
            <h4 style="color: #f1f5f9; margin-top: 0;">Pipeline Stages</h4>
            
            <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin-top: 1rem;">
                <div>
                    <h5 style="color: #f59e0b; margin-bottom: 0.5rem;">🟡 Bronze</h5>
                    <p style="color: #cbd5e1; font-size: 0.9rem; margin: 0;">
                        Raw data ingestion from Kafka.<br>
                        No validation, all data preserved.
                    </p>
                </div>
                
                <div>
                    <h5 style="color: #3b82f6; margin-bottom: 0.5rem;">🔵 Silver</h5>
                    <p style="color: #cbd5e1; font-size: 0.9rem; margin: 0;">
                        Cleaned and validated data.<br>
                        Business logic applied.
                    </p>
                </div>
                
                <div>
                    <h5 style="color: #10b981; margin-bottom: 0.5rem;">🟢 Gold</h5>
                    <p style="color: #cbd5e1; font-size: 0.9rem; margin: 0;">
                        Aggregated KPIs and metrics.<br>
                        Ready for consumption.
                    </p>
                </div>
            </div>
            
            <div style="margin-top: 1.5rem; padding: 1rem; background: rgba(15, 23, 42, 0.5); border-radius: 6px;">
                <h5 style="color: #f1f5f9; margin-bottom: 0.5rem;">📈 Delta Lake Features</h5>
                <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 0.5rem; font-size: 0.9rem;">
                    <span style="color: #cbd5e1;">• Time Travel</span>
                    <span style="color: #cbd5e1;">• Schema Evolution</span>
                    <span style="color: #cbd5e1;">• ACID Transactions</span>
                    <span style="color: #cbd5e1;">• MERGE Operations</span>
                    <span style="color: #cbd5e1;">• VACUUM</span>
                    <span style="color: #cbd5e1;">• Z-Ordering</span>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab4:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.markdown("### 🏗️ System Architecture")
            
            st.markdown("""
            ```mermaid
            graph TB
                A[Kafka Producer] --> B[Stock Data Streaming]
                B --> C[Bronze Layer<br/>Raw Data]
                C --> D[Spark Streaming]
                D --> E[Silver Layer<br/>Cleaned Data]
                E --> F[Spark ML]
                E --> G[Spark SQL]
                F --> H[Gold Layer<br/>KPIs & Predictions]
                G --> H
                H --> I[Streamlit Dashboard]
                H --> J[REST API]
                H --> K[BI Tools]
                
                style A fill:#f59e0b
                style C fill:#f59e0b
                style E fill:#3b82f6
                style H fill:#10b981
                style I fill:#8b5cf6
            ```
            """)
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.markdown("### ⚙️ Technical Stack")
            
            tech_stack = {
                "Data Ingestion": ["Kafka", "Python Producer"],
                "Stream Processing": ["Apache Spark", "Spark Streaming"],
                "Data Lake": ["Delta Lake", "Bronze/Silver/Gold"],
                "Machine Learning": ["Spark MLlib", "Random Forest"],
                "Visualization": ["Streamlit", "Plotly"],
                "Orchestration": ["Docker", "Docker Compose"]
            }
            
            for category, technologies in tech_stack.items():
                with st.expander(f"📦 {category}", expanded=True):
                    for tech in technologies:
                        st.markdown(f"• **{tech}**")
            
            st.markdown("""
            <div style="margin-top: 1.5rem; padding: 1rem; background: rgba(15, 23, 42, 0.5); border-radius: 8px;">
                <h5 style="color: #f1f5f9; margin-bottom: 0.5rem;">🎯 Project Objectives</h5>
                <ul style="color: #cbd5e1; font-size: 0.9rem; padding-left: 1rem;">
                    <li>Real-time data ingestion with Kafka</li>
                    <li>Multi-layer Delta Lake architecture</li>
                    <li>Spark Streaming for ETL</li>
                    <li>Machine Learning with Spark MLlib</li>
                    <li>Real-time visualization dashboard</li>
                    <li>Scalable and fault-tolerant design</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown('</div>', unsafe_allow_html=True)
    
    # Auto-refresh logic
    if auto_refresh:
        refresh_seconds = 5  # Default 5 seconds
        time_since_refresh = (datetime.now() - st.session_state.last_refresh).seconds
        
        if time_since_refresh >= refresh_seconds:
            st.session_state.last_refresh = datetime.now()
            st.cache_data.clear()
            st.rerun()
        
        # Show refresh countdown
        time_until_refresh = refresh_seconds - time_since_refresh
        st.sidebar.markdown(f"""
        <div style="color: #94a3b8; font-size: 0.8rem; text-align: center; margin-top: 1rem;">
            Next refresh in: <span style="color: #60a5fa;">{time_until_refresh}s</span>
        </div>
        """, unsafe_allow_html=True)

# ============================================================================
# RUN APP
# ============================================================================
if __name__ == "__main__":
    main()