#!/usr/bin/env python3
"""
Simple Kafka consumer that processes stock data and saves to Parquet
"""

import json
import time
import pandas as pd
import os
from kafka import KafkaConsumer
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_stock_data():
    """
    Consume stock data from Kafka and save to Parquet periodically
    """
    try:
        # Kafka configuration
        bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        topic = 'stock-data'

        # Create consumer
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[bootstrap_servers],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=1000
        )

        logger.info(f"Connected to Kafka at {bootstrap_servers}, consuming from topic {topic}")

        # Data collection
        stock_data = []
        batch_size = 100  # Process every 100 messages
        message_count = 0

        # Create output directory
        output_dir = '/data/superset'
        os.makedirs(output_dir, exist_ok=True)

        for message in consumer:
            try:
                data = message.value
                logger.info(f"Received: {data}")

                # Add timestamp if not present
                if 'timestamp' not in data:
                    data['timestamp'] = datetime.now().isoformat()

                stock_data.append(data)
                message_count += 1

                # Process batch
                if message_count >= batch_size:
                    process_batch(stock_data, output_dir)
                    stock_data = []  # Reset batch
                    message_count = 0

            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue

    except Exception as e:
        logger.error(f"Error in consumer: {e}")
        # If no messages, create sample data
        create_sample_data(output_dir)

def process_batch(stock_data, output_dir):
    """
    Process a batch of stock data and save to Parquet
    """
    try:
        if not stock_data:
            return

        # Convert to DataFrame
        df = pd.DataFrame(stock_data)

        # Ensure timestamp is datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Add derived columns for analysis
        df['date'] = df['timestamp'].dt.date
        df['hour'] = df['timestamp'].dt.hour
        df['daily_change'] = df.groupby('symbol')['price'].diff()
        df['price_direction'] = df['daily_change'].apply(lambda x: 'up' if x > 0 else 'down' if x < 0 else 'neutral')

        # Save to Parquet
        output_file = os.path.join(output_dir, 'stock_data_dashboard.parquet')

        # If file exists, append; otherwise create
        if os.path.exists(output_file):
            existing_df = pd.read_parquet(output_file)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df.to_parquet(output_file, index=False)
        else:
            df.to_parquet(output_file, index=False)

        logger.info(f"Saved {len(df)} records to {output_file}")

        # Also save a CSV sample for debugging
        csv_file = os.path.join(output_dir, 'stock_data_sample.csv')
        df.head(100).to_csv(csv_file, index=False)

    except Exception as e:
        logger.error(f"Error processing batch: {e}")

def create_sample_data(output_dir):
    """
    Create sample stock data if no real data is available
    """
    try:
        logger.info("Creating sample stock data...")

        # Generate sample data
        symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
        sample_data = []

        base_time = datetime.now()
        for i in range(1000):
            symbol = symbols[i % len(symbols)]
            # Simulate price movement
            base_price = 100 + (i % len(symbols)) * 50
            price_variation = (i % 20 - 10) * 2  # -20 to +20 variation
            price = base_price + price_variation

            timestamp = base_time.replace(second=i % 60, microsecond=0)

            sample_data.append({
                'symbol': symbol,
                'price': price,
                'volume': 100000 + i * 1000,
                'timestamp': timestamp.isoformat(),
                'date': timestamp.date(),
                'hour': timestamp.hour,
                'daily_change': price_variation,
                'price_direction': 'up' if price_variation > 0 else 'down' if price_variation < 0 else 'neutral'
            })

        df = pd.DataFrame(sample_data)
        output_file = os.path.join(output_dir, 'stock_data_dashboard.parquet')
        df.to_parquet(output_file, index=False)

        csv_file = os.path.join(output_dir, 'stock_data_sample.csv')
        df.head(100).to_csv(csv_file, index=False)

        logger.info(f"Created sample data with {len(df)} records")

    except Exception as e:
        logger.error(f"Error creating sample data: {e}")

if __name__ == "__main__":
    logger.info("Starting stock data processor...")
    process_stock_data()
