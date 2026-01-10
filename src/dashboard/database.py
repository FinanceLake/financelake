import duckdb
import streamlit as st
from dashboard import config

@st.cache_resource
def get_db_connection():
    """
    Creates a persistent DuckDB connection acting as a SQL Engine over S3.
    """
    try:
        con = duckdb.connect(database=':memory:')
        
        # Install S3 Extension
        con.execute("INSTALL httpfs; LOAD httpfs;")
        
        # Configure MinIO Access
        con.execute(f"""
            SET s3_endpoint='{config.S3_ENDPOINT}';
            SET s3_access_key_id='{config.ACCESS_KEY}';
            SET s3_secret_access_key='{config.SECRET_KEY}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)
        
        # Initialize Views (Virtual Tables) for ALL layers
        # This allows you to run "SELECT * FROM bronze"
        tables = {
            "bronze": config.BRONZE_PATH,
            "silver": config.SILVER_PATH,
            "gold": config.GOLD_PATH
        }
        
        for name, path in tables.items():
            try:
                con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{path}/symbol=*/*.parquet', hive_partitioning=true)")
            except:
                pass # Table might not exist yet

        # Predictions is flat (not partitioned by symbol usually in this setup)
        try:
            con.execute(f"CREATE OR REPLACE VIEW predictions AS SELECT * FROM read_parquet('{config.PRED_PATH}/*.parquet')")
        except:
            pass
            
        return con
    except Exception as e:
        st.error(f"Failed to connect to Data Lake: {e}")
        return None