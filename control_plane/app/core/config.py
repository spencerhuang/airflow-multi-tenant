"""Configuration settings for the control plane service."""

from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.

    Attributes:
        PROJECT_NAME: Name of the project
        VERSION: API version
        API_V1_STR: API v1 prefix
        DATABASE_URL: Database connection URL
        AIRFLOW_API_URL: Airflow REST API base URL
        AIRFLOW_USERNAME: Airflow API username
        AIRFLOW_PASSWORD: Airflow API password
        SECRET_KEY: Secret key for JWT encoding
        ALGORITHM: JWT algorithm
        ACCESS_TOKEN_EXPIRE_MINUTES: JWT token expiration time
        AUTH_ENABLED: Enable/disable authentication
        KAFKA_BOOTSTRAP_SERVERS: Kafka bootstrap servers
        KAFKA_TOPIC_CDC: Kafka topic for CDC events
    """

    PROJECT_NAME: str = "Airflow Multi-Tenant Control Plane"
    VERSION: str = "1.0.0"
    API_V1_STR: str = "/api/v1"

    # Database
    DATABASE_URL: str = "mysql+aiomysql://control_plane:control_plane@localhost:3306/control_plane"

    # Airflow
    AIRFLOW_API_URL: str = "http://localhost:8080/api/v1"
    AIRFLOW_USERNAME: str = "airflow"
    AIRFLOW_PASSWORD: str = "airflow"

    # Security
    SECRET_KEY: str = "your-secret-key-here-change-in-production"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    AUTH_ENABLED: bool = False  # Disabled for now as per requirements

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_CDC: str = "cdc.integration.events"
    KAFKA_TOPIC_DLQ: str = "cdc.integration.events.dlq"

    # Dead Letter Queue configuration
    KAFKA_DLQ_ENABLED: bool = True
    KAFKA_DLQ_MAX_RETRIES: int = 3

    # Backfill Strategy (Section 9 of spec)
    MAX_BACKFILL_DAYS: int = 7
    MAX_BACKFILL_RUNS_PER_INTEGRATION: int = 7
    BACKFILL_BATCH_SIZE: int = 10
    BACKFILL_BATCH_DELAY_SECONDS: int = 5
    BACKFILL_INITIAL_DELAY_SECONDS: int = 5
    BACKFILL_MAX_DELAY_SECONDS: int = 120
    BACKFILL_JITTER_SECONDS: int = 5
    BACKFILL_STAGGER_INTERVAL_MS: int = 100

    # DST Handling (Section 7 of spec)
    DEFAULT_TIMEZONE: str = "UTC"
    DST_TRANSITION_CHECK_ENABLED: bool = True

    # Worker and Scheduler Configuration
    # These can override Airflow DAG defaults if needed
    WORKER_CONCURRENCY: Optional[int] = None
    SCHEDULER_PARSING_PROCESSES: Optional[int] = None

    # MinIO / S3 Configuration
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_SECURE: bool = False

    # MongoDB Configuration
    MONGO_HOST: str = "localhost"
    MONGO_PORT: int = 27017
    MONGO_USERNAME: Optional[str] = None
    MONGO_PASSWORD: Optional[str] = None

    # OpenTelemetry
    OTEL_ENABLED: bool = True
    OTEL_SERVICE_NAME: str = "control-plane-service"
    OTEL_EXPORTER_ENDPOINT: Optional[str] = None

    # Logging
    LOG_LEVEL: str = "INFO"  # DEBUG, INFO, WARNING, ERROR
    LOG_FORMAT: str = "json"  # json or text

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",  # Ignore extra env vars (like AIRFLOW_UID for docker-compose)
    )


settings = Settings()
