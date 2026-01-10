import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
from config import LOGO_PATH, GOLD_PREDICTIONS_PATH
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.colors as mcolors

# ==========================================
# 1. APP CONFIGURATION
# ==========================================
st.set_page_config(
    page_title="FinanceLake Terminal",
    page_icon="", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==========================================
# 2. INJECT CSS
# ==========================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap');
</style>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
""", unsafe_allow_html=True)

# Default theme set to 'light'
if 'theme' not in st.session_state: st.session_state.theme = 'light'
if 'symbol_page' not in st.session_state: st.session_state.symbol_page = 0
if 'is_running' not in st.session_state: st.session_state.is_running = True

def toggle_theme():
    st.session_state.theme = 'dark' if st.session_state.theme == 'light' else 'light'

def toggle_stream():
    st.session_state.is_running = not st.session_state.is_running

def load_css(theme):
    if theme == 'dark':
        # --- DARK MODE ---
        bg_color = "#0b0e11"
        main_bg_gradient = "linear-gradient(145deg, #0b0e11 0%, #151a21 100%)"
        
        card_bg = "rgba(30, 34, 45, 0.6)"
        card_border = "rgba(255, 255, 255, 0.08)"
        
        text_color = "#e0e6ed" 
        secondary_text = "#94a3b8"
        footer_color = "#94a3b8" 
        
        accent_color = "#3b82f6"      
        success_color = "#10b981"     
        danger_color = "#ef4444"      
        
        shadow = "0 8px 32px 0 rgba(0, 0, 0, 0.3)"
        sidebar_bg = "#080a0c"
        chart_grid_color = "rgba(255,255,255,0.06)"
        table_bg = "#0f1319"
        backdrop_filter = "blur(10px)"
        scroll_track = "#0b0e11"
        scroll_thumb = "#334155"
        col_sma10 = "#f59e0b" 
        col_sma50 = "#3b82f6"
        
        extra_input_css = """
        div[data-baseweb="select"] > div {
            background-color: rgba(30, 34, 45, 0.6) !important;
            border-color: rgba(255, 255, 255, 0.08) !important;
            color: #e0e6ed !important;
        }
        div[data-baseweb="popover"], div[data-baseweb="menu"] {
            background-color: #151a21 !important;
            border: 1px solid rgba(255, 255, 255, 0.08) !important;
        }
        li[role="option"] {
            background-color: #151a21 !important;
            color: #e0e6ed !important;
        }
        div[data-testid="stElementToolbar"] {
             background-color: rgba(30, 34, 45, 0.6) !important;
             border: 1px solid rgba(255, 255, 255, 0.08) !important;
        }
        div[data-testid="stElementToolbar"] button {
             color: #e0e6ed !important;
        }
        div[data-testid="stElementToolbar"] svg {
             fill: #e0e6ed !important;
        }
        """
        
    else:
        # --- LIGHT MODE  ---
        bg_color = "#f1f5f9" 
        main_bg_gradient = "linear-gradient(180deg, #f1f5f9 0%, #ffffff 100%)"
        
        card_bg = "#ffffff"
        card_border = "#cbd5e1" 
        
        text_color = "#0f172a"        
        secondary_text = "#64748b"    
        footer_color = "#000000"      
        
        accent_color = "#2563eb"
        success_color = "#059669"
        danger_color = "#dc2626"
        
        shadow = "0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px -1px rgba(0, 0, 0, 0.1)"
        sidebar_bg = "#ffffff"
        chart_grid_color = "rgba(0,0,0,0.08)"
        table_bg = "#ffffff"
        backdrop_filter = "none"
        scroll_track = "#f1f5f9"
        scroll_thumb = "#94a3b8"
        col_sma10 = "#d97706" 
        col_sma50 = "#1d4ed8"
        
        # --- LIGHT MODE OVERRIDES ---
        extra_input_css = f"""
        /* 1. DROPDOWNS */
        div[data-baseweb="select"] > div {{
            background-color: #ffffff !important;
            color: {text_color} !important;
            border: 1px solid {card_border} !important;
        }}
        div[data-baseweb="popover"] {{
            background-color: #ffffff !important;
            border: 1px solid {card_border} !important;
        }}
        div[data-baseweb="menu"] {{
            background-color: #ffffff !important;
        }}
        li[role="option"] {{
            background-color: #ffffff !important;
            color: {text_color} !important;
        }}
        li[role="option"] * {{
            background-color: transparent !important;
            color: {text_color} !important;
        }}
        li[role="option"]:hover, li[role="option"][aria-selected="true"] {{
            background-color: #f1f5f9 !important;
            color: {accent_color} !important;
        }}
        li[role="option"]:hover *, li[role="option"][aria-selected="true"] * {{
            color: {accent_color} !important;
        }}

        /* 2. TABLE TOOLBAR (Gray Icons) */
        div[data-testid="stElementToolbar"] {{
            background-color: #ffffff !important;
            border: 1px solid {card_border} !important;
            box-shadow: 0 1px 2px rgba(0,0,0,0.1) !important;
        }}
        div[data-testid="stElementToolbar"] button {{
            background-color: transparent !important;
            color: {secondary_text} !important;
            border: none !important;
        }}
        div[data-testid="stElementToolbar"] svg {{
            fill: {secondary_text} !important;
            color: {secondary_text} !important;
        }}
        div[data-testid="stElementToolbar"] button:hover {{
            background-color: #f1f5f9 !important;
            color: {accent_color} !important;
        }}
        div[data-testid="stElementToolbar"] button:hover svg {{
            fill: {accent_color} !important;
        }}

        /* 3. TOOLTIPS */
        div[data-baseweb="tooltip"] {{
            background-color: #1e293b !important;
            color: #ffffff !important;
            border-radius: 4px !important;
            padding: 4px 8px !important;
            border: 1px solid {card_border} !important;
        }}
        div[data-baseweb="tooltip"] * {{
            color: #ffffff !important; 
            background-color: transparent !important;
            font-size: 0.85rem !important;
        }}

        /* 4. EXPANDER FIX (Force Dark Text) */
        div[data-testid="stExpander"] {{
            background-color: #ffffff !important;
            border: 1px solid {card_border} !important;
            color: {text_color} !important;
        }}
        div[data-testid="stExpander"] summary {{
            color: {text_color} !important;
            background-color: #ffffff !important;
        }}
        /* Force the inner paragraph and icon to be dark */
        div[data-testid="stExpander"] summary p, 
        div[data-testid="stExpander"] summary span,
        div[data-testid="stExpander"] summary [data-testid="stIconMaterial"] {{
            color: {text_color} !important;
            fill: {text_color} !important;
        }}
        
        /* Expander Hover */
        div[data-testid="stExpander"] summary:hover {{
            color: {accent_color} !important;
        }}
        div[data-testid="stExpander"] summary:hover p,
        div[data-testid="stExpander"] summary:hover span,
        div[data-testid="stExpander"] summary:hover [data-testid="stIconMaterial"] {{
            color: {accent_color} !important;
            fill: {accent_color} !important;
        }}

        /* 5. TABLE HEADERS */
        [data-testid="stDataFrame"] th {{
            background-color: #ffffff !important;
            color: {text_color} !important;
            border-bottom: 1px solid {card_border} !important;
        }}
        """

    st.markdown(f"""
    <style>
        html, body, [class*="css"] {{
            font-family: 'Inter', sans-serif;
            color: {text_color};
            background-color: {bg_color};
        }}
        .stApp {{
            background: {bg_color};
            background-image: {main_bg_gradient};
            background-attachment: fixed;
        }}
        .block-container {{
            padding-top: 2rem;
            padding-bottom: 5rem;
        }}
        
        /* SCROLLBAR */
        ::-webkit-scrollbar {{ width: 8px; height: 8px; }}
        ::-webkit-scrollbar-track {{ background: {scroll_track}; }}
        ::-webkit-scrollbar-thumb {{ background: {scroll_thumb}; border-radius: 4px; }}
        ::-webkit-scrollbar-thumb:hover {{ background: {accent_color}; }}

        /* SIDEBAR */
        section[data-testid="stSidebar"] {{
            background-color: {sidebar_bg};
            border-right: 1px solid {card_border};
            box-shadow: 2px 0 12px rgba(0,0,0,0.05);
        }}
        section[data-testid="stSidebar"] h1, h2, h3, span, p, li, div {{
            color: {text_color} !important;
        }}
        
        .logo-container img {{
            filter: drop-shadow(0 0 8px {accent_color}40);
            transition: transform 0.3s ease;
        }}
        .logo-container img:hover {{ transform: scale(1.05); }}

        /* GENERAL CARDS */
        .stMetric, .stPlotlyChart, .ticker-wrap, .sidebar-box {{
            background: {card_bg};
            backdrop-filter: {backdrop_filter};
            -webkit-backdrop-filter: {backdrop_filter};
            border: 1px solid {card_border};
            border-radius: 8px;
            box-shadow: {shadow};
            padding: 1rem;
            transition: all 0.2s ease;
        }}
        
        .stMetric:hover, .sidebar-box:hover {{
            transform: translateY(-2px);
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -4px rgba(0, 0, 0, 0.1);
            border-color: {accent_color}60;
        }}

        /* METRICS */
        div[data-testid="stMetricLabel"] p {{
            color: {secondary_text} !important;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}
        div[data-testid="stMetricValue"] {{
            font-family: 'JetBrains Mono', monospace;
            font-weight: 700;
            font-size: 1.5rem !important;
            color: {text_color} !important;
        }}

        /* TICKER */
        .ticker-wrap {{
            width: 100%; overflow: hidden; padding: 12px 0; margin-bottom: 24px; white-space: nowrap; position: relative;
        }}
        .ticker-item {{
            display: inline-block; padding: 0 2rem;
            font-family: 'JetBrains Mono', monospace; font-size: 0.9rem;
            color: {text_color};
            border-right: 1px solid {card_border};
        }}

        /* BUTTONS */
        .stButton button {{
            background: {card_bg};
            border: 1px solid {card_border};
            color: {text_color};
            font-weight: 600;
            border-radius: 8px;
            transition: all 0.2s;
        }}
        .stButton button:hover {{
            border-color: {accent_color};
            color: {accent_color};
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }}
        button[kind="primary"] {{
            background: {accent_color} !important;
            color: white !important;
            border: none !important;
        }}
        button[kind="primary"]:hover {{
            opacity: 0.9;
            box-shadow: 0 4px 12px {accent_color}60;
        }}

        h1, h2, h3, h4 {{
            color: {text_color} !important;
            font-family: 'Inter', sans-serif;
            font-weight: 700;
            letter-spacing: -0.02em;
        }}
        
        .bull {{ color: {success_color} !important; }}
        .bear {{ color: {danger_color} !important; }}
        .accent {{ color: {accent_color} !important; }}
        
        /* INJECT EXTRA CSS */
        {extra_input_css}

        #MainMenu {{visibility: hidden;}}
        footer {{visibility: hidden;}}
        header {{visibility: hidden;}}
        
    </style>
    """, unsafe_allow_html=True)
    
    return text_color, chart_grid_color, success_color, danger_color, accent_color, table_bg, col_sma10, col_sma50, footer_color

theme_text, chart_grid, col_bull, col_bear, col_accent, table_bg, col_sma10, col_sma50, footer_color = load_css(st.session_state.theme)

# ==========================================
# 3. SPARK ENGINE
# ==========================================
# Use path from config
PREDICTIONS_PATH = GOLD_PREDICTIONS_PATH

@st.cache_resource
def get_spark():
    return SparkSession.builder \
        .appName("FinanceLake_Pro") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

spark = get_spark()

# ==========================================
# 4. INDICATORS LOGIC
# ==========================================
def calculate_advanced_indicators(df):
    if df.empty: return df
    df = df.sort_values('timestamp')
    
    # SMA & Bands
    df['SMA_10'] = df['avg_price'].rolling(window=10).mean()
    df['SMA_50'] = df['avg_price'].rolling(window=50).mean()
    df['SMA_20'] = df['avg_price'].rolling(window=20).mean()
    df['std_dev'] = df['avg_price'].rolling(window=20).std()
    df['BB_Upper'] = df['SMA_20'] + (df['std_dev'] * 2)
    df['BB_Lower'] = df['SMA_20'] - (df['std_dev'] * 2)

    # MACD
    exp1 = df['avg_price'].ewm(span=12, adjust=False).mean()
    exp2 = df['avg_price'].ewm(span=26, adjust=False).mean()
    df['MACD'] = exp1 - exp2
    df['Signal_Line'] = df['MACD'].ewm(span=9, adjust=False).mean()

    # Stochastic
    low_14 = df['avg_price'].rolling(window=14).min()
    high_14 = df['avg_price'].rolling(window=14).max()
    df['%K'] = 100 * ((df['avg_price'] - low_14) / (high_14 - low_14))
    df['%D'] = df['%K'].rolling(window=3).mean()
    
    # OBV
    if 'avg_volume' not in df.columns: df['avg_volume'] = 1000 
    df['OBV'] = (np.sign(df['avg_price'].diff()) * df['avg_volume']).fillna(0).cumsum()

    return df

def fetch_data():
    try:
        df_spark = spark.read.format("delta").load(PREDICTIONS_PATH)
        cols_available = df_spark.columns
        select_expr = [
            col("window.end").alias("timestamp"),
            col("symbol"),
            col("avg_price"),
            col("prediction"),
            col("probability")
        ]
        if "avg_volume" in cols_available: select_expr.append(col("avg_volume"))
        
        df_clean = df_spark.select(*select_expr)
        
        df_clean = df_clean.orderBy(col("timestamp").desc()).limit(5000)
        
        pdf = df_clean.toPandas()
        if not pdf.empty and 'probability' in pdf.columns:
            pdf['probability'] = pdf['probability'].apply(lambda x: float(x[1]) if x else 0.0)
        return pdf
    except:
        return pd.DataFrame()

# ==========================================
# 5. LAYOUT & UI
# ==========================================

# --- SIDEBAR ---
with st.sidebar:
    st.markdown('<div class="logo-container">', unsafe_allow_html=True)
    c_logo_l, c_logo_m, c_logo_r = st.columns([1, 2, 1])
    with c_logo_m:
        try:
            st.image(LOGO_PATH, use_container_width=True)
        except Exception:
            st.warning("Logo not found")
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown(f"""
    <div style="text-align: center; margin-bottom: 30px;">
        <h2 style="margin: 10px 0 0 0; font-size: 1.4rem; font-weight:800; letter-spacing: -1px;">FinanceLake</h2>
        <div style="font-family:'JetBrains Mono'; font-size: 0.7rem; color:{col_accent}; margin-top:5px; border:1px solid {col_accent}; display:inline-block; padding:2px 8px; border-radius:4px;">TERMINAL v2.0</div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown(f"""
    <div class="sidebar-box">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
            <span style="font-weight:600; font-size:0.9rem;"><i class="fas fa-server accent"></i> System Status</span>
            <span style="background-color:{col_bull}20; color:{col_bull}; padding: 2px 8px; border-radius: 4px; font-weight:700; font-size: 0.65rem; border: 1px solid {col_bull}40;">ONLINE</span>
        </div>
        <div style="font-size:0.8rem; color:{theme_text}; opacity:0.8; line-height:1.8;">
            <div style="display:flex; align-items:center;">
                <i class="fas fa-circle" style="width:20px; color:{col_bull}; font-size:0.4rem; display:flex; align-items:center; justify-content:center;"></i> 
                Kafka Ingestion
            </div>
            <div style="display:flex; align-items:center;">
                <i class="fas fa-circle" style="width:20px; color:{col_bull}; font-size:0.4rem; display:flex; align-items:center; justify-content:center;"></i> 
                Spark Cluster
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("### Controls")
    refresh_rate = st.slider("Latency (sec)", 1, 10, 2)
    
    if st.session_state.is_running:
        btn_label = "⏸ Pause Feed"
        btn_type = "secondary"
    else:
        btn_label = "▶ Resume Feed"
        btn_type = "primary"
        
    if st.button(btn_label, type=btn_type, use_container_width=True):
        toggle_stream()
        st.rerun()
    
    st.markdown("---")
    # UPDATED FOOTER TEXT AND COLOR
    st.markdown(f"""
    <div style='text-align:center; font-size:0.75rem; margin-top:20px; color: {footer_color}; font-weight: 600; opacity: 0.9;'>
        &copy; 2025 FinanceLake<br>
        Developed by Big Data & AI Class 2024
    </div>
    """, unsafe_allow_html=True)

# --- DATA CHECK ---
raw_df = fetch_data()
if raw_df.empty:
    st.warning("📡 Establishing Uplink to Data Stream...")
    time.sleep(2)
    st.rerun()

# --- HEADER AREA ---
col_title, col_theme = st.columns([10, 1]) 
with col_title:
    st.markdown(f"### <i class='fas fa-chart-line accent'></i> Market Overview", unsafe_allow_html=True)
with col_theme:
    btn_icon = "🌙" if st.session_state.theme == 'light' else "☀️"
    if st.button(f"{btn_icon}", help="Toggle Theme"):
        toggle_theme()
        st.rerun()

# --- A. TICKER TAPE ---
latest_snapshot = raw_df.sort_values('timestamp').groupby('symbol').tail(1)
html = "<div class='ticker-wrap'><marquee scrollamount='6'>"
for _, row in latest_snapshot.iterrows():
    if row['prediction'] == 1.0:
        icon_html = f'<i class="fas fa-caret-up" style="color:{col_bull};"></i>'
        price_style = f'color:{col_bull};'
    else:
        icon_html = f'<i class="fas fa-caret-down" style="color:{col_bear};"></i>'
        price_style = f'color:{col_bear};'
    
    html += f"<span class='ticker-item'><span style='opacity:0.6; font-weight:500;'>{row['symbol']}</span>&nbsp;&nbsp;{icon_html} <span style='{price_style} font-weight:700;'>${row['avg_price']:.2f}</span></span>"
html += "</marquee></div>"
st.markdown(html, unsafe_allow_html=True)

# --- B. TOP ACTIVE ASSETS ---
all_symbols = latest_snapshot['symbol'].unique()
items_per_page = 4
total_pages = int(np.ceil(len(all_symbols) / items_per_page))

col_head, col_nav = st.columns([8, 2])
with col_head:
    pass 
with col_nav:
    c_prev, c_txt, c_next = st.columns([1, 2, 1])
    with c_prev:
        if st.button("◄", key="prev"): 
            st.session_state.symbol_page = (st.session_state.symbol_page - 1) % total_pages
            st.rerun()
    with c_next:
        if st.button("►", key="next"):
            st.session_state.symbol_page = (st.session_state.symbol_page + 1) % total_pages
            st.rerun()
    with c_txt:
         st.markdown(f"<div style='text-align:center; margin-top:8px; font-family:JetBrains Mono; font-size:0.8rem; opacity:0.6;'>{st.session_state.symbol_page + 1} / {total_pages}</div>", unsafe_allow_html=True)

start_idx = st.session_state.symbol_page * items_per_page
end_idx = start_idx + items_per_page
current_page_symbols = all_symbols[start_idx:end_idx]

cols = st.columns(4)
for i, symbol in enumerate(current_page_symbols):
    with cols[i]:
        if i < len(current_page_symbols):
            s_data = latest_snapshot[latest_snapshot['symbol'] == current_page_symbols[i]]
            if not s_data.empty:
                row = s_data.iloc[0]
                is_bull = row['prediction'] == 1.0
                trend_arrow = "▲" if is_bull else "▼"
                trend_color = "normal" if is_bull else "inverse"
                
                st.metric(
                    label=f"{symbol}",
                    value=f"${row['avg_price']:.2f}",
                    delta=f"{trend_arrow} {row['probability']:.1%} Conf.",
                    delta_color=trend_color
                )

# --- SELECTOR ---
# --- SELECTOR ---
st.markdown("---")
st.markdown("### <i class='fas fa-microscope accent'></i> Asset Forensics", unsafe_allow_html=True)

# Sort the symbols so the list order doesn't jump around when data updates
available_symbols = sorted(raw_df['symbol'].unique())

# We also check if the previously selected symbol is still in the new data list
if "selected_asset" not in st.session_state:
    st.session_state.selected_asset = available_symbols[0] if len(available_symbols) > 0 else None

# Ensure current selection is valid
index_to_use = 0
if st.session_state.selected_asset in available_symbols:
    index_to_use = available_symbols.index(st.session_state.selected_asset)

def update_selected_asset():
    st.session_state.selected_asset = st.session_state.asset_selector

selected_symbol = st.selectbox(
    "Select Asset", 
    available_symbols, 
    index=index_to_use,
    key="asset_selector",
    on_change=update_selected_asset,
    label_visibility="collapsed"
)

# --- CHART PREP ---
asset_df = raw_df[raw_df['symbol'] == selected_symbol].copy()
asset_df = calculate_advanced_indicators(asset_df)

def style_chart(fig, height=350):
    fig.update_layout(
        height=height, 
        paper_bgcolor='rgba(0,0,0,0)', 
        plot_bgcolor='rgba(0,0,0,0)',
        # 1. Force Global Font Color
        font=dict(color=theme_text, family="JetBrains Mono", size=10),
        margin=dict(l=10, r=10, t=30, b=10),
        legend=dict(
            orientation="h", 
            y=1.02, 
            x=0, 
            bgcolor='rgba(0,0,0,0)',
            bordercolor=chart_grid, 
            borderwidth=1,
            # 2. FORCE LEGEND TEXT COLOR
            font=dict(color=theme_text)
        ),
        hovermode="x unified",
        hoverlabel=dict(
            bgcolor=table_bg, 
            # 3. Ensure Hover Text matches theme
            font=dict(color=theme_text, family="Inter", size=12)
        )
    )
    # Ensure axes colors match
    fig.update_xaxes(showgrid=True, gridcolor=chart_grid, zeroline=False, showline=True, linecolor=chart_grid, tickfont=dict(color=theme_text))
    fig.update_yaxes(showgrid=True, gridcolor=chart_grid, zeroline=False, showline=True, linecolor=chart_grid, tickfont=dict(color=theme_text))
    return fig

# --- C. ISOLATED CHARTS ---
c1, c2 = st.columns(2)
with c1:
    st.markdown(f"**Trend Analysis** <span style='opacity:0.5; font-size:0.8rem'>(SMA Cross)</span>", unsafe_allow_html=True)
    fig1 = go.Figure()
    fig1.add_trace(go.Scatter(x=asset_df['timestamp'], y=asset_df['avg_price'], name='Price', line=dict(color=theme_text, width=2)))
    fig1.add_trace(go.Scatter(x=asset_df['timestamp'], y=asset_df['SMA_10'], name='SMA 10', line=dict(color=col_sma10, width=1.5)))
    fig1.add_trace(go.Scatter(x=asset_df['timestamp'], y=asset_df['SMA_50'], name='SMA 50', line=dict(color=col_sma50, width=1.5)))
    st.plotly_chart(style_chart(fig1), use_container_width=True,theme=None)

with c2:
    st.markdown(f"**Volatility** <span style='opacity:0.5; font-size:0.8rem'>(Bollinger Bands)</span>", unsafe_allow_html=True)
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=asset_df['timestamp'], y=asset_df['BB_Upper'], name='Upper', line=dict(width=0), showlegend=False))
    fig2.add_trace(go.Scatter(x=asset_df['timestamp'], y=asset_df['BB_Lower'], name='Band', fill='tonexty', fillcolor='rgba(128,128,128,0.1)', line=dict(width=0)))
    fig2.add_trace(go.Scatter(x=asset_df['timestamp'], y=asset_df['avg_price'], name='Price', line=dict(color=col_accent, width=2)))
    st.plotly_chart(style_chart(fig2), use_container_width=True,theme=None)

c3, c4 = st.columns(2)
with c3:
    st.markdown(f"**Volume Strength** <span style='opacity:0.5; font-size:0.8rem'>(OBV)</span>", unsafe_allow_html=True)
    fig3 = make_subplots(specs=[[{"secondary_y": True}]])
    vol_col = 'avg_volume' if 'avg_volume' in asset_df.columns else 'probability'
    colors_vol = [col_bull if r['prediction']==1 else col_bear for i, r in asset_df.iterrows()]
    
    fig3.add_trace(go.Bar(x=asset_df['timestamp'], y=asset_df[vol_col], name='Vol', marker_color=colors_vol, opacity=0.3), secondary_y=False)
    fig3.add_trace(go.Scatter(x=asset_df['timestamp'], y=asset_df['OBV'], name='OBV', line=dict(color='#8b5cf6', width=2)), secondary_y=True)
    
    fig3 = style_chart(fig3)
    fig3.update_yaxes(gridcolor=chart_grid, secondary_y=False, showgrid=False)
    st.plotly_chart(fig3, use_container_width=True,theme=None)

with c4:
    st.markdown(f"**Momentum** <span style='opacity:0.5; font-size:0.8rem'>(Stochastic)</span>", unsafe_allow_html=True)
    fig4 = go.Figure()
    fig4.add_trace(go.Scatter(x=asset_df['timestamp'], y=asset_df['%K'], name='%K', line=dict(color='#0ea5e9', width=1.5)))
    fig4.add_trace(go.Scatter(x=asset_df['timestamp'], y=asset_df['%D'], name='%D', line=dict(color='#ec4899', width=1.5)))
    fig4.add_hline(y=80, line_dash="dot", line_color=chart_grid, opacity=0.5)
    fig4.add_hline(y=20, line_dash="dot", line_color=chart_grid, opacity=0.5)
    st.plotly_chart(style_chart(fig4), use_container_width=True,theme=None)

st.markdown(f"**AI Confidence & MACD Convergence**", unsafe_allow_html=True)
fig5 = go.Figure()
fig5.add_trace(go.Bar(x=asset_df['timestamp'], y=asset_df['probability'], name='AI Prob', marker_color=col_accent, opacity=0.15, yaxis='y1'))
fig5.add_trace(go.Scatter(x=asset_df['timestamp'], y=asset_df['MACD'], name='MACD', line=dict(color=col_bull, width=2), yaxis='y2'))
fig5.add_trace(go.Scatter(x=asset_df['timestamp'], y=asset_df['Signal_Line'], name='Signal', line=dict(color=col_bear, width=2), yaxis='y2'))

fig5 = style_chart(fig5, height=350)
fig5.update_layout(
    yaxis=dict(title="Probability", range=[0, 1], side="left", showgrid=False), 
    yaxis2=dict(title="MACD", overlaying="y", side="right", showgrid=True, gridcolor=chart_grid)
)
st.plotly_chart(fig5, use_container_width=True,theme=None)

# --- D. TABLE ---
st.markdown("### <i class='fas fa-table brand-icon'></i> Consolidated Ledger", unsafe_allow_html=True)
with st.expander("Expand Data View", expanded=True):
    full_table = raw_df.copy()
    full_table['Signal'] = full_table['prediction'].map({1.0: 'BUY', 0.0: 'SELL'})
    
    # DETERMINE GRADIENT COLOR MAP
    if st.session_state.theme == 'light':
        # Standard Traffic Light (Bright Red -> Bright Green)
        cmap_choice = "RdYlGn" 
    else:
        # Custom Dark Mode Gradient (Deep Crimson -> Dark Olive -> Deep Emerald)
        colors = ["#450a0a", "#422006", "#052e16"] 
        cmap_choice = mcolors.LinearSegmentedColormap.from_list("DarkConfidence", colors)

    st.dataframe(
        full_table.sort_values('timestamp', ascending=False)
        .style.set_properties(**{'background-color': table_bg, 'color': theme_text, 'border-color': chart_grid})
        .background_gradient(subset=['probability'], cmap=cmap_choice, vmin=0, vmax=1)
        .applymap(lambda x: f"color: {'#10b981' if x=='BUY' else '#ef4444'}; font-weight:bold", subset=['Signal'])
        .format({"avg_price": "${:.2f}", "probability": "{:.1%}"}),
        column_config={
            "probability": st.column_config.NumberColumn(
                "Confidence",
                help="The model's calculated probability (0-100%) that the price direction prediction is correct.",
            ),
            "Signal": st.column_config.TextColumn("AI Decision"),
        },
        use_container_width=True,
        hide_index=True
    )

# --- LOOP CONTROL ---
if st.session_state.is_running:
    time.sleep(refresh_rate)
    st.rerun()