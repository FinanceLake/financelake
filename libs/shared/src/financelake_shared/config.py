# ============================================================================
# Configuration Management
# ============================================================================
# Enterprise-grade configuration management with validation and secrets handling
# ============================================================================

import os
from functools import lru_cache
from typing import List, Optional, Union

from pydantic import BaseSettings, Field, validator


class Settings(BaseSettings):
    """
    Application settings with environment variable binding and validation.

    This class follows the 12-factor app methodology and provides centralized
    configuration management for all microservices.
    """

    # ============================================================================
    # APPLICATION SETTINGS
    # ============================================================================

    app_name: str = Field(default="financelake", description="Application name")
    app_version: str = Field(default="1.0.0", description="Application version")
    environment: str = Field(
        default="dev",
        description="Environment (dev, staging, prod)",
        env="ENVIRONMENT"
    )
    debug: bool = Field(default=False, description="Enable debug mode", env="DEBUG")

    # ============================================================================
    # SERVICE DISCOVERY
    # ============================================================================

    service_name: str = Field(description="Current service name", env="SERVICE_NAME")
    service_port: int = Field(default=8000, description="Service port", env="SERVICE_PORT")
    service_host: str = Field(default="0.0.0.0", description="Service host", env="SERVICE_HOST")

    # ============================================================================
    # KAFKA CONFIGURATION
    # ============================================================================

    kafka_bootstrap_servers: List[str] = Field(
        default=["kafka:9092"],
        description="Kafka bootstrap servers",
        env="KAFKA_BOOTSTRAP_SERVERS"
    )
    kafka_security_protocol: str = Field(
        default="PLAINTEXT",
        description="Kafka security protocol",
        env="KAFKA_SECURITY_PROTOCOL"
    )
    kafka_sasl_mechanism: Optional[str] = Field(
        default=None,
        description="Kafka SASL mechanism",
        env="KAFKA_SASL_MECHANISM"
    )
    kafka_sasl_username: Optional[str] = Field(
        default=None,
        description="Kafka SASL username",
        env="KAFKA_SASL_USERNAME"
    )
    kafka_sasl_password: Optional[str] = Field(
        default=None,
        description="Kafka SASL password",
        env="KAFKA_SASL_PASSWORD"
    )

    # ============================================================================
    # DATABASE CONFIGURATION
    # ============================================================================

    # PostgreSQL for metadata and application state
    postgres_host: str = Field(
        default="postgres",
        description="PostgreSQL host",
        env="POSTGRES_HOST"
    )
    postgres_port: int = Field(
        default=5432,
        description="PostgreSQL port",
        env="POSTGRES_PORT"
    )
    postgres_database: str = Field(
        default="financelake",
        description="PostgreSQL database name",
        env="POSTGRES_DATABASE"
    )
    postgres_username: str = Field(
        default="financelake",
        description="PostgreSQL username",
        env="POSTGRES_USERNAME"
    )
    postgres_password: str = Field(
        default="password",
        description="PostgreSQL password",
        env="POSTGRES_PASSWORD"
    )
    postgres_ssl_mode: str = Field(
        default="prefer",
        description="PostgreSQL SSL mode",
        env="POSTGRES_SSL_MODE"
    )

    # Iceberg Catalog (REST or JDBC)
    iceberg_catalog_type: str = Field(
        default="rest",
        description="Iceberg catalog type (rest, jdbc, hive)",
        env="ICEBERG_CATALOG_TYPE"
    )
    iceberg_rest_uri: str = Field(
        default="http://iceberg-rest:8181",
        description="Iceberg REST catalog URI",
        env="ICEBERG_REST_URI"
    )
    iceberg_warehouse: str = Field(
        default="s3://financelake-data/warehouse",
        description="Iceberg warehouse location",
        env="ICEBERG_WAREHOUSE"
    )

    # ============================================================================
    # OBJECT STORAGE (S3, GCS, etc.)
    # ============================================================================

    storage_backend: str = Field(
        default="s3",
        description="Object storage backend (s3, gcs, azure)",
        env="STORAGE_BACKEND"
    )
    storage_bucket: str = Field(
        description="Storage bucket name",
        env="STORAGE_BUCKET"
    )
    storage_region: str = Field(
        default="us-east-1",
        description="Storage region",
        env="STORAGE_REGION"
    )
    storage_access_key: Optional[str] = Field(
        default=None,
        description="Storage access key",
        env="STORAGE_ACCESS_KEY"
    )
    storage_secret_key: Optional[str] = Field(
        default=None,
        description="Storage secret key",
        env="STORAGE_SECRET_KEY"
    )

    # ============================================================================
    # MONITORING & OBSERVABILITY
    # ============================================================================

    # Prometheus metrics
    metrics_enabled: bool = Field(
        default=True,
        description="Enable Prometheus metrics",
        env="METRICS_ENABLED"
    )
    metrics_port: int = Field(
        default=9090,
        description="Metrics server port",
        env="METRICS_PORT"
    )

    # Distributed tracing
    tracing_enabled: bool = Field(
        default=True,
        description="Enable distributed tracing",
        env="TRACING_ENABLED"
    )
    tracing_service_name: str = Field(
        default="financelake-service",
        description="Service name for tracing",
        env="TRACING_SERVICE_NAME"
    )
    otel_exporter_otlp_endpoint: Optional[str] = Field(
        default=None,
        description="OTLP exporter endpoint",
        env="OTEL_EXPORTER_OTLP_ENDPOINT"
    )

    # Logging
    log_level: str = Field(
        default="INFO",
        description="Logging level",
        env="LOG_LEVEL"
    )
    log_format: str = Field(
        default="json",
        description="Log format (json, text)",
        env="LOG_FORMAT"
    )

    # ============================================================================
    # SECURITY
    # ============================================================================

    # JWT tokens
    jwt_secret_key: str = Field(
        description="JWT secret key",
        env="JWT_SECRET_KEY"
    )
    jwt_algorithm: str = Field(
        default="HS256",
        description="JWT algorithm",
        env="JWT_ALGORITHM"
    )
    jwt_access_token_expire_minutes: int = Field(
        default=30,
        description="JWT access token expiration in minutes",
        env="JWT_ACCESS_TOKEN_EXPIRE_MINUTES"
    )

    # CORS
    cors_origins: List[str] = Field(
        default=["http://localhost:3000", "http://localhost:8501"],
        description="CORS allowed origins",
        env="CORS_ORIGINS"
    )

    # ============================================================================
    # ML/FEATURE STORE SETTINGS
    # ============================================================================

    mlflow_tracking_uri: str = Field(
        default="http://mlflow:5000",
        description="MLflow tracking server URI",
        env="MLFLOW_TRACKING_URI"
    )
    feast_core_uri: str = Field(
        default="feast-core:6565",
        description="Feast feature store core URI",
        env="FEAST_CORE_URI"
    )
    feast_online_store_uri: str = Field(
        default="redis://redis:6379",
        description="Feast online store URI",
        env="FEAST_ONLINE_STORE_URI"
    )

    # ============================================================================
    # EXTERNAL SERVICES
    # ============================================================================

    # Redis for caching and sessions
    redis_url: str = Field(
        default="redis://redis:6379",
        description="Redis connection URL",
        env="REDIS_URL"
    )

    # Elasticsearch for search and analytics
    elasticsearch_hosts: List[str] = Field(
        default=["elasticsearch:9200"],
        description="Elasticsearch hosts",
        env="ELASTICSEARCH_HOSTS"
    )

    # ============================================================================
    # BUSINESS LOGIC SETTINGS
    # ============================================================================

    # Data processing
    batch_size: int = Field(
        default=1000,
        description="Default batch size for processing",
        env="BATCH_SIZE"
    )
    processing_timeout_seconds: int = Field(
        default=300,
        description="Processing timeout in seconds",
        env="PROCESSING_TIMEOUT_SECONDS"
    )

    # Stock symbols to track
    tracked_symbols: List[str] = Field(
        default=["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "NFLX"],
        description="Stock symbols to track",
        env="TRACKED_SYMBOLS"
    )

    # ============================================================================
    # VALIDATORS
    # ============================================================================

    @validator("kafka_bootstrap_servers", pre=True)
    def parse_kafka_servers(cls, v):
        """Parse comma-separated Kafka bootstrap servers."""
        if isinstance(v, str):
            return [x.strip() for x in v.split(",")]
        return v

    @validator("cors_origins", pre=True)
    def parse_cors_origins(cls, v):
        """Parse comma-separated CORS origins."""
        if isinstance(v, str):
            return [x.strip() for x in v.split(",")]
        return v

    @validator("elasticsearch_hosts", pre=True)
    def parse_elasticsearch_hosts(cls, v):
        """Parse comma-separated Elasticsearch hosts."""
        if isinstance(v, str):
            return [x.strip() for x in v.split(",")]
        return v

    @validator("tracked_symbols", pre=True)
    def parse_tracked_symbols(cls, v):
        """Parse comma-separated stock symbols."""
        if isinstance(v, str):
            return [x.strip() for x in v.split(",")]
        return v

    @validator("environment")
    def validate_environment(cls, v):
        """Validate environment value."""
        valid_environments = ["dev", "staging", "prod"]
        if v not in valid_environments:
            raise ValueError(f"Environment must be one of: {valid_environments}")
        return v

    @validator("log_level")
    def validate_log_level(cls, v):
        """Validate log level."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of: {valid_levels}")
        return v.upper()

    # ============================================================================
    # PROPERTIES
    # ============================================================================

    @property
    def database_url(self) -> str:
        """PostgreSQL database URL."""
        return (
            f"postgresql://{self.postgres_username}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_database}"
            f"?sslmode={self.postgres_ssl_mode}"
        )

    @property
    def kafka_config(self) -> dict:
        """Kafka client configuration."""
        config = {
            "bootstrap_servers": self.kafka_bootstrap_servers,
            "security_protocol": self.kafka_security_protocol,
        }

        if self.kafka_sasl_mechanism:
            config.update({
                "sasl_mechanism": self.kafka_sasl_mechanism,
                "sasl_plain_username": self.kafka_sasl_username,
                "sasl_plain_password": self.kafka_sasl_password,
            })

        return config

    @property
    def is_production(self) -> bool:
        """Check if running in production."""
        return self.environment == "prod"

    @property
    def is_development(self) -> bool:
        """Check if running in development."""
        return self.environment == "dev"

    # ============================================================================
    # CLASS CONFIG
    # ============================================================================

    class Config:
        """Pydantic configuration."""
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached application settings.

    This function provides a singleton instance of settings that can be
    imported and used throughout the application.

    Returns:
        Settings: Application settings instance
    """
    return Settings()
