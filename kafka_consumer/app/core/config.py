"""Configuration settings for the Kafka CDC consumer service."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    PROJECT_NAME: str = "Kafka CDC Consumer"
    VERSION: str = "1.0.0"

    # Database (sync pymysql — no async needed)
    DATABASE_URL: str = "mysql+pymysql://control_plane:control_plane@localhost:3306/control_plane"

    # Airflow
    AIRFLOW_API_URL: str = "http://localhost:8080/api/v2"
    AIRFLOW_USERNAME: str = "airflow"
    AIRFLOW_PASSWORD: str = "airflow"

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_CDC: str = "cdc.integration.events"
    KAFKA_TOPIC_DLQ: str = "cdc.integration.events.dlq"
    KAFKA_DLQ_ENABLED: bool = True
    KAFKA_DLQ_MAX_RETRIES: int = 3
    KAFKA_DLQ_DB_ENABLED: bool = True
    KAFKA_CONSUMER_GROUP: str = "cdc-consumer"

    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )


settings = Settings()
