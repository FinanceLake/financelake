#!/usr/bin/env python3
"""
Prepare stock data for Superset visualization
"""

import pandas as pd
import os
import logging
from pathlib import Path
import duckdb

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def prepare_stock_data():
    """
    Load Parquet files and prepare them for Superset
    """
    try:
        # Define paths
        data_dir = Path(__file__).parent.parent / "data" / "processed"
        output_dir = Path(__file__).parent.parent / "data" / "superset"
        output_dir.mkdir(exist_ok=True)
        
        # Find all parquet files
        parquet_files = list(data_dir.glob("*.parquet"))
        
        if not parquet_files:
            logger.error("No parquet files found in data/processed/")
            return
            
        logger.info(f"Found {len(parquet_files)} parquet files")
        
        # Use DuckDB to combine and process data
        conn = duckdb.connect(':memory:')
        
        # Create a view combining all parquet files
        parquet_pattern = str(data_dir / "*.parquet")
        
        # Load data and add basic transformations
        query = f"""
        CREATE VIEW stock_data AS
        SELECT 
            *,
            DATE_TRUNC('day', timestamp) as date,
            EXTRACT(year FROM timestamp) as year,
            EXTRACT(month FROM timestamp) as month,
            EXTRACT(dow FROM timestamp) as day_of_week,
            (high - low) as daily_range,
            (close - open) as daily_change,
            CASE 
                WHEN close > open THEN 'gain'
                WHEN close < open THEN 'loss'
                ELSE 'neutral'
            END as price_direction
        FROM read_parquet('{parquet_pattern}')
        WHERE timestamp IS NOT NULL
        ORDER BY timestamp;
        """
        
        conn.execute(query)
        
        # Export to a single parquet file for Superset
        output_file = output_dir / "stock_data_dashboard.parquet"
        conn.execute(f"COPY stock_data TO '{output_file}' (FORMAT PARQUET)")
        
        # Also create a CSV version for easier debugging
        csv_file = output_dir / "stock_data_dashboard.csv"
        conn.execute(f"COPY (SELECT * FROM stock_data LIMIT 10000) TO '{csv_file}' (FORMAT CSV, HEADER)")
        
        # Get basic statistics
        stats = conn.execute("SELECT COUNT(*) as total_rows, MIN(timestamp) as min_date, MAX(timestamp) as max_date FROM stock_data").fetchone()
        
        logger.info(f"Data preparation completed:")
        logger.info(f"  - Total rows: {stats[0]}")
        logger.info(f"  - Date range: {stats[1]} to {stats[2]}")
        logger.info(f"  - Output file: {output_file}")
        logger.info(f"  - CSV sample: {csv_file}")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Error preparing data: {e}")
        raise

if __name__ == "__main__":
    prepare_stock_data()