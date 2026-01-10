import streamlit as st
import time
from dashboard import config
from dashboard import services
from dashboard import ui_components
from dashboard import sql_page  # <--- Import new page

# 1. Setup Page
st.set_page_config(page_title=config.PAGE_TITLE, page_icon=config.PAGE_ICON, layout=config.LAYOUT)

# 2. Navigation
st.sidebar.title(f"{config.PAGE_ICON} FinanceLake")
page = st.sidebar.radio("Navigation", ["Live Dashboard", "SQL Playground"])

if page == "SQL Playground":
    sql_page.render_sql_page()
    st.stop() # Stop processing the rest of the file if on SQL page

# ==========================================
# LIVE DASHBOARD LOGIC (Existing Code)
# ==========================================

st.sidebar.header("📡 Data Feed")

# Fetch symbols
symbols = services.get_available_symbols()

if not symbols:
    st.warning("⏳ Waiting for data stream...")
    time.sleep(3)
    st.rerun()

# Session State for Symbol
if 'selected_symbol' not in st.session_state:
    st.session_state.selected_symbol = symbols[0]

selected_symbol = st.sidebar.selectbox(
    "Select Asset", 
    options=symbols,
    index=symbols.index(st.session_state.selected_symbol) if st.session_state.selected_symbol in symbols else 0,
    key='symbol_selector'
)

if st.session_state.symbol_selector != st.session_state.selected_symbol:
    st.session_state.selected_symbol = st.session_state.symbol_selector
    st.rerun()

auto_refresh = st.sidebar.checkbox("Live Updates", value=True)

st.title(f"📈 Real-Time Market Monitor")
dashboard_placeholder = st.empty()

def update_dashboard():
    with dashboard_placeholder.container():
        df = services.get_market_data(st.session_state.selected_symbol)
        prediction = services.get_latest_prediction(st.session_state.selected_symbol)
        
        if df is not None:
            ui_components.render_kpis(df, prediction)
            st.divider()
            ui_components.render_advanced_chart(df, st.session_state.selected_symbol)
        else:
            st.info(f"Waiting for data for {st.session_state.selected_symbol}...")

if auto_refresh:
    update_dashboard()
    time.sleep(config.REFRESH_SECONDS)
    st.rerun()
else:
    update_dashboard()