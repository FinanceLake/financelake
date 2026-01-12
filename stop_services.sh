#!/usr/bin/env bash
set -euo pipefail

# Derive WORK_DIR from this script location (makes the script portable)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_DIR="${WORK_DIR:-$SCRIPT_DIR}"
LOG_DIR="$WORK_DIR/log"

echo "=============================================="
echo "🛑 Stopping all pipeline services..."
echo "=============================================="

stop_pid_file() {
    local pid_file="$1"
    local service_name="$2"
    if [ -f "$pid_file" ]; then
        pid=$(cat "$pid_file")
        if ps -p $pid > /dev/null 2>&1; then
            echo "Stopping $service_name (PID $pid)..."
            kill $pid
            # wait up to 10s for graceful shutdown
            for i in {1..10}; do
                if ! ps -p $pid > /dev/null 2>&1; then
                    echo "✓ $service_name stopped"
                    break
                fi
                sleep 1
            done
            if ps -p $pid > /dev/null 2>&1; then
                echo "⚠️ $service_name did not stop gracefully; forcing kill"
                kill -9 $pid
            fi
        else
            echo "$service_name PID $pid not running"
        fi
        rm -f "$pid_file"
    else
        echo "PID file for $service_name not found: $pid_file"
    fi
}

# Stop services in reverse order of startup
stop_pid_file "$LOG_DIR/spark_consumer.pid" "Spark Consumer"
stop_pid_file "$LOG_DIR/kafka_producer.pid" "Kafka Producer"
stop_pid_file "$LOG_DIR/web_app.pid" "Web App"
stop_pid_file "$LOG_DIR/kafka.pid" "Kafka Broker"
stop_pid_file "$LOG_DIR/zookeeper.pid" "Zookeeper"

echo "=============================================="
echo "✅ All services stopped (or attempts made)."
echo "=============================================="
