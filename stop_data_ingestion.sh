#!/bin/bash

echo "Stopping streaming producer (producer.py)..."
pkill -f producer.py

echo "Stopping Kafka, Zookeeper, NiFi, and other Docker services..."
docker compose down

echo "All ingestion processes and related services have been stopped."