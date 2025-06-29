from kafka import KafkaConsumer
import json
from config import KAFKA_TOPIC, KAFKA_SERVER, HDFS_LOG_PATH
import subprocess

LOG_FILE = "/tmp/transactions.log"
HDFS_DIR = HDFS_LOG_PATH

# Clear existing log file
with open(LOG_FILE, 'w') as f:
    f.write("")

try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=None
    )

    print("Consuming from Kafka and writing to local file...")

    counter = 0 
    batch_size = 10

    with open(LOG_FILE, 'a') as log_file:
        for message in consumer:
            record = json.dumps(message.value)
            print("Received:", record)
            log_file.write(record + "\n")
            counter += 1

            if counter % batch_size == 0:
                print(f"Uploading to HDFS after {counter} records...")
                try:
                    # Remove old file if exists in HDFS
                    subprocess.run(["hdfs", "dfs", "-rm", f"{HDFS_DIR}/transactions.log"], check=False)
                    # Upload new log file
                    subprocess.run(["hdfs", "dfs", "-put", "-f", LOG_FILE, HDFS_DIR], check=True)
                    print("Uploaded to HDFS")
                except subprocess.CalledProcessError as e:
                    print(" Error uploading to HDFS:", e)

except Exception as e:
    print(" Error consuming from Kafka:", e)
