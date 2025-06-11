import os
from datetime import timedelta

# Security
SECRET_KEY = os.environ.get('SECRET_KEY', 'your_SECRET_KEY_')
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None

# Database
SQLALCHEMY_DATABASE_URI = f"postgresql://{os.environ.get('DATABASE_USER', 'superset')}:{os.environ.get('DATABASE_PASSWORD', 'superset')}@{os.environ.get('DATABASE_HOST', 'postgres')}:{os.environ.get('DATABASE_PORT', '5432')}/{os.environ.get('DATABASE_DB', 'superset')}"

# Redis
REDIS_HOST = os.environ.get('REDIS_HOST', 'redis')
REDIS_PORT = os.environ.get('REDIS_PORT', 6379)

# Cache
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': REDIS_HOST,
    'CACHE_REDIS_PORT': REDIS_PORT,
    'CACHE_REDIS_DB': 1,
}

# Celery
CELERY_CONFIG = {
    'broker_url': f'redis://{REDIS_HOST}:{REDIS_PORT}/0',
    'imports': ['superset.sql_lab'],
    'result_backend': f'redis://{REDIS_HOST}:{REDIS_PORT}/0',
    'worker_prefetch_multiplier': 1,
    'task_acks_late': False,
    'task_annotations': {
        'sql_lab.get_sql_results': {
            'rate_limit': '100/s',
        },
    },
}

# Feature flags
FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
    'DASHBOARD_NATIVE_FILTERS': True,
    'DASHBOARD_CROSS_FILTERS': True,
    'DASHBOARD_FILTERS_EXPERIMENTAL': True,
}

# Row limit
ROW_LIMIT = 5000
VIZ_ROW_LIMIT = 10000

# Timeout
SUPERSET_WEBSERVER_TIMEOUT = 300

# Email
SMTP_HOST = 'localhost'
SMTP_STARTTLS = True
SMTP_SSL = False
SMTP_USER = 'superset'
SMTP_PORT = 25
SMTP_PASSWORD = 'superset'
SMTP_MAIL_FROM = 'superset@localhost'

# WebDriver
WEBDRIVER_BASEURL = 'http://superset:8088'
WEBDRIVER_BASEURL_USER_FRIENDLY = 'http://localhost:8088'

# Additional database engines
SQLALCHEMY_CUSTOM_PASSWORD_STORE = None

# Enable DuckDB for Parquet files
SQLALCHEMY_ENGINES = {
    'duckdb': 'duckdb_engine',
}