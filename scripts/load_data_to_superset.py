#!/usr/bin/env python3
"""
Load stock data from Parquet into PostgreSQL for Superset access
"""

import pandas as pd
import psycopg2
import os
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_data_to_postgres():
    """
    Load Parquet data into PostgreSQL for Superset
    """
    try:
        # Database connection parameters
        db_params = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'database': os.getenv('POSTGRES_DB', 'financelake'),
            'user': os.getenv('POSTGRES_USER', 'financelake'),
            'password': os.getenv('POSTGRES_PASSWORD', 'financelake_password'),
            'port': '5432'
        }

        # Connect to PostgreSQL
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        logger.info("Connected to PostgreSQL")

        # Create table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS stock_data (
            symbol VARCHAR(10),
            open_price DECIMAL(10,2),
            high_price DECIMAL(10,2),
            low_price DECIMAL(10,2),
            close_price DECIMAL(10,2),
            volume BIGINT,
            timestamp TIMESTAMP,
            date DATE,
            hour INTEGER,
            daily_change DECIMAL(10,2),
            price_direction VARCHAR(10)
        );
        """
        cursor.execute(create_table_query)
        conn.commit()

        # Load data from Parquet
        parquet_file = '../data/superset/stock_data_dashboard.parquet'
        if not os.path.exists(parquet_file):
            # Try absolute path from project root
            parquet_file = 'data/superset/stock_data_dashboard.parquet'
        if os.path.exists(parquet_file):
            df = pd.read_parquet(parquet_file)
            logger.info(f"Loaded {len(df)} rows from Parquet file")

            # Insert data
            for _, row in df.iterrows():
                insert_query = """
                INSERT INTO stock_data (symbol, open_price, high_price, low_price, close_price, volume, timestamp, date, hour, daily_change, price_direction)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
                """
                cursor.execute(insert_query, (
                    row.get('symbol'),
                    row.get('open'),
                    row.get('high'),
                    row.get('low'),
                    row.get('close'),
                    row.get('volume', 0),
                    row.get('timestamp'),
                    row.get('date'),
                    row.get('hour', 0),
                    row.get('daily_change'),
                    row.get('price_direction')
                ))

            conn.commit()
            logger.info("Data loaded successfully into PostgreSQL")
        else:
            logger.warning(f"Parquet file not found: {parquet_file}")

        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

if __name__ == "__main__":
    load_data_to_postgres()
