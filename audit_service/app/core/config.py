"""Configuration settings for the Audit Service.

Uses the unified secret provider for sensitive fields.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import model_validator
from typing import Optional

from shared_utils.secret_provider import read_secret


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    PROJECT_NAME: str = "Audit Service"
    VERSION: str = "1.0.0"

    # Database (sync pymysql — consumer thread is sync)
    DATABASE_URL: str = "mysql+pymysql://audit_svc:audit_svc@localhost:3306/audit_template"

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC: str = "audit.events"
    KAFKA_CONSUMER_GROUP: str = "audit-consumer"

    # Logging
    LOG_LEVEL: str = "INFO"

    @model_validator(mode="after")
    def _resolve_secrets(self) -> "Settings":
        """Override sensitive fields from the unified secret provider."""
        db_url = read_secret("AUDIT_DATABASE_URL")
        if db_url:
            object.__setattr__(self, "DATABASE_URL", db_url)
        return self

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )


settings = Settings()
