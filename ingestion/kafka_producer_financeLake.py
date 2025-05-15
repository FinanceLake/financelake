import json
import requests
import time
import traceback
import os
from kafka import KafkaProducer
from datetime import datetime
from hdfs import InsecureClient
from config import TOPIC, HDFS_BASE_DIR, LOCAL_LOG_DIR, API_KEY, SYMBOL, TOPIC_NAME, KAFKA_BOOTSTRAP_SERVERS, HDFS_URL

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

# Initialize HDFS client
hdfs_client = InsecureClient(HDFS_URL , user='hadoop')

os.makedirs(LOCAL_LOG_DIR, exist_ok=True)

def get_hdfs_path():
    now = datetime.now()
    date_str = now.strftime('%d-%m-%y')
    hour_str = now.strftime('%H%M')
    second_str = now.strftime('%S')
    ts = int(now.timestamp() * 1000)
    return f"{HDFS_BASE_DIR}{date_str}/{hour_str}/{second_str}/events-{ts}.log"

def clean_alpha_vantage_data(raw_data):
    clean_data = {}
    if 'Meta Data' in raw_data:
        meta = raw_data['Meta Data']
        clean_meta = {k.split('. ', 1)[-1].strip().lower().replace(' ', '_'): v for k, v in meta.items()}
        clean_data['meta_data'] = clean_meta
    else:
        clean_data['meta_data'] = {}

    time_series_key = next((k for k in raw_data if k.lower().startswith('time series')), None)
    if time_series_key:
        time_series = raw_data[time_series_key]
        clean_time_series = {}
        for timestamp, values in time_series.items():
            clean_values = {k.split('. ', 1)[-1].strip().lower(): v for k, v in values.items()}
            clean_time_series[timestamp] = clean_values
        clean_data['time_series'] = clean_time_series
    else:
        clean_data['time_series'] = {}

    return clean_data

while True:
    try:
        # Fetch data from Alpha Vantage API
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={SYMBOL}&interval=1min&apikey={API_KEY}'
        response = requests.get(url)
        data = response.json()
        cleaned_data = clean_alpha_vantage_data(data)
        print(cleaned_data)

        if "Time Series (1min)" in data:
            producer.send(TOPIC_NAME, cleaned_data)
            print(f"Sent data for {SYMBOL} at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print(f"Error from API: {data.get('Note') or data.get('Error Message') or 'Unknown error'}")

    except Exception as e:
        now = datetime.now()
        hdfs_path = get_hdfs_path()
        error_msg = f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {traceback.format_exc()}\n"
        print(f"Writing error to HDFS: {hdfs_path}")

        local_log_file = f"{LOCAL_LOG_DIR}/error_{int(now.timestamp())}.log"
        with open(local_log_file, 'w') as f:
            f.write(error_msg)

        try:
            hdfs_client.makedirs(os.path.dirname(hdfs_path), create_parents=True)
            with open(local_log_file, 'rb') as log_file:
                hdfs_client.write(hdfs_path, log_file, overwrite=True)
            print(f"Error log written to HDFS path: {hdfs_path}")
        except Exception as hdfs_error:
            print(f"Failed to write to HDFS: {str(hdfs_error)}")

    # Wait for 60 seconds before the next request
    time.sleep(60)