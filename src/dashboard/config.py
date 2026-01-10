import os

# Page Config
PAGE_TITLE = "FinanceLake AI Terminal"
PAGE_ICON = "🦅"
LAYOUT = "wide"

# Refresh Rate
REFRESH_SECONDS = 2

# S3 Paths (DuckDB uses s3://)
BASE_URI = "s3://finance-lake/lake"
BRONZE_PATH = f"{BASE_URI}/bronze"
SILVER_PATH = f"{BASE_URI}/silver"
GOLD_PATH = f"{BASE_URI}/gold"
PRED_PATH = f"{BASE_URI}/predictions"

# MinIO Credentials
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "minio:9000").replace("http://", "")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")