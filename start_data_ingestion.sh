#!/bin/bash

# Start Kafka, Zookeeper, and NiFi via Docker Compose
echo "Starting Kafka, Zookeeper, and NiFi via Docker Compose..."
docker compose up -d

# Wait a few seconds for services to be ready
echo "Waiting 20 seconds for services to initialize..."
sleep 20

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
while ! nc -z localhost 9092; do
  sleep 2
done
echo "Kafka is up!"

# Start streaming ingestion in the background
echo "Starting streaming producer..."
cd data_ingestion/streaming
pip install -r requirements.txt
nohup python3 producer.py > streaming.log 2>&1 &

cd ../..

echo "Streaming ingestion (in background) and NiFi started."
echo "Access the NiFi web interface at: http://localhost:8080/nifi"
echo "Check streaming.log for streaming logs."