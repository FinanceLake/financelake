#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------
# Minimal, robust startup script for local dev pipeline
# - Kafka (Zookeeper + Broker)
# - optional Hadoop (if installed)
# - Web app (app.py)
# - Kafka producer
# - Spark consumer (spark-submit)
# ------------------------------------------------------

# Configuration (edit as needed)
export WORK_DIR="/home/qwty/delta_lake"
export HADOOP_HOME="/home/qwty/hadoop"                    # optional - only used if present
export SPARK_HOME="/home/qwty/spark"
export KAFKA_HOME="/home/qwty/kafka"
export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"     # adjust for your JVM

# Ensure important executables are available via PATH
export PATH="$PATH:$HADOOP_HOME/bin:$SPARK_HOME/bin:$KAFKA_HOME/bin"

# App-specific environment
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export KAFKA_TOPIC="data_stock"
export WEB_APP_URL="http://localhost:5000"

LOG_DIR="$WORK_DIR/log"
mkdir -p "$LOG_DIR"

echo "=============================================="
echo "🚀 Starting real-time trading pipeline"
echo "=============================================="

# --- Basic checks ---
echo "📋 Checking installation directories..."
[ -d "$SPARK_HOME" ] && echo "✓ Spark: $SPARK_HOME" || echo "✗ Spark not found ($SPARK_HOME)"
[ -d "$KAFKA_HOME" ] && echo "✓ Kafka: $KAFKA_HOME" || echo "✗ Kafka not found ($KAFKA_HOME)"
[ -d "$HADOOP_HOME" ] && echo "✓ Hadoop: $HADOOP_HOME (will attempt start)" || echo "⚪ Hadoop not found (skipping HDFS/YARN)"

# --- Stop previously running processes (best-effort) ---
echo "🛑 Stopping any previous runs..."
pkill -f "kafka.Kafka" || true
pkill -f "quorum.QuorumPeerMain" || true
pkill -f "python.*app.py" || true
pkill -f "python.*kafka_producer.py" || true
pkill -f "python.*spark_delta_consumer_v0.py" || true
sleep 3

# --- Start Zookeeper ---
if [ -d "$KAFKA_HOME" ]; then
  echo "🐘 Starting Zookeeper..."
  nohup "$KAFKA_HOME/bin/zookeeper-server-start.sh" "$KAFKA_HOME/config/zookeeper.properties" \
    > "$LOG_DIR/zookeeper.log" 2>&1 &
  echo $! > "$LOG_DIR/zookeeper.pid"
  sleep 8
else
  echo "⚠️ Kafka home not found; skipping Zookeeper start"
fi

# --- Start Kafka broker ---
if [ -d "$KAFKA_HOME" ]; then
  echo "📊 Starting Kafka broker..."
  nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/server.properties" \
    > "$LOG_DIR/kafka.log" 2>&1 &
  echo $! > "$LOG_DIR/kafka.pid"
  sleep 10
fi

# --- Helper: wait for port ---
wait_for_port(){
  host=${1:-localhost}
  port=${2:-9092}
  timeout=${3:-60}
  echo "🔎 Waiting up to ${timeout}s for ${host}:${port}..."
  start_ts=$(date +%s)
  while :; do
    # try bash TCP first
    if (echo > /dev/tcp/${host}/${port}) >/dev/null 2>&1; then
      echo "✓ ${host}:${port} reachable"
      return 0
    fi
    now_ts=$(date +%s)
    if [ $((now_ts - start_ts)) -ge $timeout ]; then
      echo "⏱️ Timed out waiting for ${host}:${port}"
      return 1
    fi
    sleep 1
  done
}

# --- Create topic (if Kafka is up) ---
if wait_for_port "localhost" 9092 60; then
  echo "📝 (Re)creating Kafka topic: $KAFKA_TOPIC"
  # delete if exists (ignore failure), then create
  "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --topic "$KAFKA_TOPIC" --delete 2>/dev/null || true
  "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --create --topic "$KAFKA_TOPIC" --partitions 1 --replication-factor 1
  "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list
else
  echo "⚠️ Kafka not reachable; skipping topic creation"
fi

# --- Python dependencies: prefer virtualenv if present ---
echo "📦 Ensuring Python dependencies..."
if [ -d "$WORK_DIR/.venv" ]; then
  echo "🔁 Activating virtualenv at $WORK_DIR/.venv"
  # shellcheck source=/dev/null
  source "$WORK_DIR/.venv/bin/activate"
  pip install --upgrade pip
  pip install yfinance kafka-python flask requests pyspark findspark --upgrade
else
  echo "⚪ No virtualenv found at $WORK_DIR/.venv — attempting user install (may require network)"
  python3 -m pip install --user yfinance kafka-python flask requests pyspark findspark || true
fi

# --- Optional Hadoop start ---
if [ -d "$HADOOP_HOME" ]; then
  echo "🚀 Starting HDFS and YARN..."
  "$HADOOP_HOME/sbin/start-dfs.sh" > "$LOG_DIR/hdfs.log" 2>&1 || true
  sleep 5
  "$HADOOP_HOME/sbin/start-yarn.sh" > "$LOG_DIR/yarn.log" 2>&1 || true
  sleep 5
fi

# --- Start Web App ---
echo "🌐 Starting web app (app.py)..."
nohup python3 "$WORK_DIR/app.py" > "$LOG_DIR/web_app.log" 2>&1 &
echo $! > "$LOG_DIR/web_app.pid"
sleep 6

# --- Start Kafka producer ---
echo "📤 Starting Kafka producer (kafka_producer.py)..."
nohup python3 "$WORK_DIR/kafka_producer.py" > "$LOG_DIR/kafka_producer.log" 2>&1 &
echo $! > "$LOG_DIR/kafka_producer.pid"
sleep 3

# --- Start Spark consumer with proper --packages formatting ---
echo "⚡ Starting Spark consumer (spark_delta_consumer_v1.py)..."
SPARK_KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
SPARK_DELTA_PACKAGE="io.delta:delta-spark_2.12:3.2.0"
SPARK_PACKAGES="${SPARK_KAFKA_PACKAGE},${SPARK_DELTA_PACKAGE}"
CONSUMER_SCRIPT="$WORK_DIR/spark_delta_consumer_v2.py"
SPARK_LOG="$LOG_DIR/spark_consumer.log"
SPARK_PID_FILE="$LOG_DIR/spark_consumer.pid"

if [ -x "$SPARK_HOME/bin/spark-submit" ] && [ -f "$CONSUMER_SCRIPT" ]; then
  # Use virtualenv python if available
  if [ -d "$WORK_DIR/.venv" ]; then
    export PYSPARK_PYTHON="$WORK_DIR/.venv/bin/python"
    export PYSPARK_DRIVER_PYTHON="$WORK_DIR/.venv/bin/python"
  fi

  nohup "$SPARK_HOME/bin/spark-submit" \
    --master local[4] \
    --packages "$SPARK_PACKAGES" \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    "$CONSUMER_SCRIPT" \
    > "$SPARK_LOG" 2>&1 &
  echo $! > "$SPARK_PID_FILE"
else
  echo "⚠️ spark-submit or consumer script missing; attempting to run consumer with python"
  nohup python3 "$CONSUMER_SCRIPT" > "$SPARK_LOG" 2>&1 &
  echo $! > "$SPARK_PID_FILE"
fi

# --- Summary & locations ---
echo ""
echo "=============================================="
echo "✅ Services started (or attempts made)."
echo "=============================================="
echo "Web App: $WEB_APP_URL"
echo "Kafka topic: $KAFKA_TOPIC"
echo ""
echo "Logs:"
echo "  - Web App:         $LOG_DIR/web_app.log"
echo "  - Kafka Producer:  $LOG_DIR/kafka_producer.log"
echo "  - Spark Consumer:  $LOG_DIR/spark_consumer.log"
echo "  - Kafka:           $LOG_DIR/kafka.log"
echo "  - Zookeeper:       $LOG_DIR/zookeeper.log"
echo ""
echo "PIDs written to $LOG_DIR/*.pid"
echo "To stop services: use your stop script (or pkill) and check the .pid files."
echo "=============================================="

# block to keep container/terminal attached when run interactively
wait
