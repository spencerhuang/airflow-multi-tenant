"""Unified secret provider: filesystem-first, env-fallback.

Resolution order for each secret:
1. File at {SECRETS_DIR}/{key_lowercase} (K8s Secret volume mount / Docker secret)
2. Environment variable {KEY_UPPERCASE}
3. Provided default value

This allows the same code to run in both Docker (env vars) and K8s (mounted secrets)
without any code changes.
"""

import os
import logging
from typing import Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Directory where K8s/Docker secrets are mounted as files
SECRETS_DIR = os.getenv("SECRETS_DIR", "/run/secrets")


def read_secret(key: str, default: Optional[str] = None) -> Optional[str]:
    """Read a secret: file first, then env var, then default.

    Args:
        key: Secret name. File lookup uses lowercase, env lookup uses uppercase.
        default: Fallback value if not found in file or env.

    Returns:
        The secret value, or default if not found.
    """
    # 1. Try filesystem (K8s Secret volume mount / Docker secret)
    file_path = os.path.join(SECRETS_DIR, key.lower())
    if os.path.isfile(file_path):
        try:
            with open(file_path) as f:
                value = f.read().strip()
            if value:
                logger.debug("Secret '%s' loaded from file", key)
                return value
        except OSError as exc:
            logger.warning("Failed to read secret file '%s': %s", file_path, exc)

    # 2. Try environment variable
    env_value = os.environ.get(key.upper()) or os.environ.get(key)
    if env_value:
        logger.debug("Secret '%s' loaded from environment", key)
        return env_value

    # 3. Fallback to default
    if default is not None:
        logger.debug("Secret '%s' using default", key)
    return default


@dataclass(frozen=True)
class InfraSecrets:
    """Infrastructure secrets resolved once at startup.

    Only infrastructure-level credentials belong here.
    Customer credentials (S3, Mongo, etc.) are per-workspace and flow
    through the Redis transient vault via PrepareTask.
    """

    # Database
    mysql_password: str
    postgres_password: str

    # Redis
    redis_password: str

    # Airflow
    airflow_fernet_key: str
    airflow_webserver_secret: str
    airflow_password: str

    # Kafka (optional — for future SASL auth)
    kafka_password: Optional[str] = None

    @classmethod
    def load(cls) -> "InfraSecrets":
        """Load all infrastructure secrets from files/env/defaults."""
        return cls(
            mysql_password=read_secret("MYSQL_PASSWORD", "control_plane"),
            postgres_password=read_secret("POSTGRES_PASSWORD", "airflow"),
            redis_password=read_secret("REDIS_PASSWORD", "changeme_redis"),
            airflow_fernet_key=read_secret("AIRFLOW_FERNET_KEY", ""),
            airflow_webserver_secret=read_secret(
                "AIRFLOW_WEBSERVER_SECRET_KEY",
                "airflow-secret-key-change-in-production",
            ),
            airflow_password=read_secret("AIRFLOW_PASSWORD", "airflow"),
            kafka_password=read_secret("KAFKA_PASSWORD"),
        )


# Module-level singleton (matches get_dag_config() pattern in airflow_config.py)
_infra_secrets: Optional[InfraSecrets] = None


def get_infra_secrets() -> InfraSecrets:
    """Get infrastructure secrets (singleton, loaded once on first call)."""
    global _infra_secrets
    if _infra_secrets is None:
        _infra_secrets = InfraSecrets.load()
    return _infra_secrets


def reset_infra_secrets() -> None:
    """Reset singleton for testing purposes."""
    global _infra_secrets
    _infra_secrets = None
