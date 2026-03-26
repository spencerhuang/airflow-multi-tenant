"""Configuration settings for the Kafka CDC consumer service.

Uses the unified secret provider for sensitive fields.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import model_validator
from typing import Optional

from shared_utils.secret_provider import read_secret


class Settings(BaseSettings):
    """Application settings loaded from environment variables.

    Sensitive fields are resolved via the unified secret provider:
    file at /run/secrets/ -> env var -> default.
    """

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
    KAFKA_PASSWORD: Optional[str] = None
    KAFKA_TOPIC_CDC: str = "cdc.integration.events"
    KAFKA_TOPIC_DLQ: str = "cdc.integration.events.dlq"
    KAFKA_DLQ_ENABLED: bool = True
    KAFKA_DLQ_MAX_RETRIES: int = 3
    KAFKA_DLQ_DB_ENABLED: bool = True
    KAFKA_CONSUMER_GROUP: str = "cdc-consumer"

    # Message deduplication
    KAFKA_DEDUP_ENABLED: bool = True
    KAFKA_DEDUP_TTL_SECONDS: int = 86400  # 24 hours — long TTL for completed keys
    KAFKA_DEDUP_CLAIM_TTL_SECONDS: int = 420  # 7 min — short lease for processing phase

    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"

    @model_validator(mode="after")
    def _resolve_secrets(self) -> "Settings":
        """Override sensitive fields from the unified secret provider."""
        airflow_pw = read_secret("AIRFLOW_PASSWORD")
        if airflow_pw:
            object.__setattr__(self, "AIRFLOW_PASSWORD", airflow_pw)

        kafka_pw = read_secret("KAFKA_PASSWORD")
        if kafka_pw:
            object.__setattr__(self, "KAFKA_PASSWORD", kafka_pw)

        return self

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )


settings = Settings()
