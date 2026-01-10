#!/bin/bash

JARS_DIR="jars"
mkdir -p "$JARS_DIR"

# List of JAR URLs (Added AWS/Hadoop dependencies)
declare -a JARS=(
  # Kafka & Delta (Keep these)
  "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.0/spark-sql-kafka-0-10_2.12-3.4.0.jar"
  "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar"
  "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
  "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.0/spark-token-provider-kafka-0-10_2.12-3.4.0.jar"
  "https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar"
  "https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar"
  "https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar"
  
  # --- NEW: S3/MinIO Support (Hadoop 3.3.4 for Spark 3.4.0) ---
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
  "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
)

for jar_url in "${JARS[@]}"; do
  filename=$(basename "$jar_url")
  if [ -f "$JARS_DIR/$filename" ]; then
    echo "$filename exists. Skipping."
  else
    echo "Downloading $filename..."
    wget -P "$JARS_DIR" "$jar_url"
  fi
done