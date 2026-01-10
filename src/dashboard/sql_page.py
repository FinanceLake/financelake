import streamlit as st
import pandas as pd
import time
from dashboard.database import get_db_connection

def render_schema_sidebar(con):
    """Helper to show available tables and columns"""
    st.sidebar.markdown("### 🗄️ Database Schema")
    
    tables = ["bronze", "silver", "gold", "predictions"]
    
    for t in tables:
        try:
            # Get columns
            df_cols = con.execute(f"DESCRIBE {t}").df()
            with st.sidebar.expander(f"🔹 {t.upper()}", expanded=False):
                st.dataframe(
                    df_cols[['column_name', 'column_type']], 
                    hide_index=True, 
                    use_container_width=True
                )
        except:
            continue

def render_sql_page():
    st.markdown("## ⚡ Spark SQL Playground")
    st.caption("Execute SQL queries directly against the Data Lake (MinIO). Powered by DuckDB engine.")
    
    con = get_db_connection()
    if not con:
        st.stop()

    # Layout
    col_editor, col_schema = st.columns([3, 1])
    
    # Render Schema in Sidebar for reference
    render_schema_sidebar(con)

    # Default Query
    default_sql = """-- Compare Moving Averages across Symbols
SELECT 
    symbol, 
    AVG(avg_price) as mean_price,
    MAX(volatility) as max_vol,
    SUM(total_volume) as total_vol
FROM gold
GROUP BY symbol
ORDER BY max_vol DESC
"""

    with st.container():
        query = st.text_area("SQL Query", value=default_sql, height=200, key="sql_input")
        
        c1, c2 = st.columns([1, 6])
        run_btn = c1.button("▶️ Run Query", type="primary")
        
        if run_btn:
            try:
                start_time = time.time()
                df_result = con.execute(query).df()
                duration = time.time() - start_time
                
                st.success(f"✅ Query executed in {duration:.3f} seconds. Returned {len(df_result)} rows.")
                st.dataframe(df_result, use_container_width=True)
                
                # Simple Auto-Chart if result fits
                if len(df_result) > 0 and len(df_result.columns) >= 2:
                    with st.expander("📊 Quick Visualization"):
                        st.bar_chart(df_result.set_index(df_result.columns[0]))
                        
            except Exception as e:
                st.error(f"SQL Error: {e}")