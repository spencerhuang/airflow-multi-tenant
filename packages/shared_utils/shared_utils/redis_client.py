"""TLS-aware Redis client singleton with transient credential vault helpers.

Environment detection:
- If CA cert file exists at REDIS_CA_CERT_PATH -> enable SSL/mTLS (production)
- Otherwise -> plain TCP (developer mode, no certs needed)

Credential vault:
- store_credentials(dag_run_id, creds) -> SET with 30-min TTL (atomic via setex)
- fetch_credentials(dag_run_id) -> GET or raise AirflowFailException on expiry
- delete_credentials(dag_run_id) -> explicit cleanup by CleanUpTask
"""

import json
import os
import logging
from typing import Optional

import redis as redis_lib

from shared_utils.secret_provider import get_infra_secrets

logger = logging.getLogger(__name__)

# TLS certificate paths (standard K8s cert-manager locations)
REDIS_CA_CERT_PATH = os.getenv("REDIS_CA_CERT_PATH", "/etc/ssl/redis/ca.crt")
REDIS_CLIENT_CERT_PATH = os.getenv("REDIS_CLIENT_CERT_PATH", "/etc/ssl/redis/tls.crt")
REDIS_CLIENT_KEY_PATH = os.getenv("REDIS_CLIENT_KEY_PATH", "/etc/ssl/redis/tls.key")

# Default TTL for transient credentials (30 minutes)
CREDENTIAL_TTL_SECONDS = int(os.getenv("CREDENTIAL_TTL_SECONDS", "1800"))

# Redis key prefix for credential isolation
_KEY_PREFIX = "airflow:run_"
_KEY_SUFFIX = ":creds"

# Singleton
_redis_client: Optional[redis_lib.Redis] = None


def get_redis_client() -> redis_lib.Redis:
    """Get a Redis client singleton with adaptive TLS.

    If REDIS_CA_CERT_PATH exists on disk, enables SSL with mTLS.
    Otherwise, connects over plain TCP (developer mode).

    Returns:
        A connected redis.Redis instance.

    Raises:
        redis.ConnectionError: If Redis is unreachable.
    """
    global _redis_client
    if _redis_client is not None:
        return _redis_client

    secrets = get_infra_secrets()
    host = os.getenv("REDIS_HOST", "redis")
    port = int(os.getenv("REDIS_PORT", "6379"))
    password = secrets.redis_password

    use_tls = os.path.isfile(REDIS_CA_CERT_PATH)

    if use_tls:
        logger.info("Connecting to Redis at %s:%d with TLS", host, port)
        _redis_client = redis_lib.Redis(
            host=host,
            port=port,
            password=password,
            ssl=True,
            ssl_ca_certs=REDIS_CA_CERT_PATH,
            ssl_certfile=REDIS_CLIENT_CERT_PATH,
            ssl_keyfile=REDIS_CLIENT_KEY_PATH,
            decode_responses=True,
        )
    else:
        logger.info("Connecting to Redis at %s:%d (plain TCP, no TLS)", host, port)
        _redis_client = redis_lib.Redis(
            host=host,
            port=port,
            password=password,
            decode_responses=True,
        )

    # Verify connectivity
    _redis_client.ping()
    logger.info("Redis connection established")
    return _redis_client


def reset_redis_client() -> None:
    """Reset the singleton for testing purposes."""
    global _redis_client
    _redis_client = None


def _credential_key(dag_run_id: str) -> str:
    """Build the Redis key for a DAG run's credentials."""
    return f"{_KEY_PREFIX}{dag_run_id}{_KEY_SUFFIX}"


def store_credentials(
    dag_run_id: str,
    credentials: dict,
    ttl: int = CREDENTIAL_TTL_SECONDS,
) -> None:
    """Store credentials in Redis with TTL (atomic write + expiration).

    Args:
        dag_run_id: Airflow DAG run ID (used as isolation key).
        credentials: Credential dict to store (serialized as JSON).
        ttl: Time-to-live in seconds (default: 1800 = 30 minutes).
    """
    client = get_redis_client()
    key = _credential_key(dag_run_id)
    client.setex(key, ttl, json.dumps(credentials))
    logger.info("Stored credentials for %s (TTL=%ds)", dag_run_id, ttl)


def fetch_credentials(dag_run_id: str) -> dict:
    """Fetch credentials from Redis.

    Args:
        dag_run_id: Airflow DAG run ID.

    Returns:
        The credential dict.

    Raises:
        AirflowFailException: If the key is missing (TTL expired or never stored).
        RuntimeError: If running outside Airflow (fallback exception).
    """
    client = get_redis_client()
    key = _credential_key(dag_run_id)
    value = client.get(key)

    if value is None:
        msg = (
            "Credentials expired. "
            "Confirm with business domain if task should be re-triggered."
        )
        # Import Airflow exception lazily to avoid import errors in non-Airflow contexts
        try:
            from airflow.exceptions import AirflowFailException
            raise AirflowFailException(msg)
        except ImportError:
            raise RuntimeError(msg)

    return json.loads(value)


def delete_credentials(dag_run_id: str) -> None:
    """Delete credentials from Redis (explicit cleanup by CleanUpTask).

    Args:
        dag_run_id: Airflow DAG run ID.
    """
    client = get_redis_client()
    key = _credential_key(dag_run_id)
    client.delete(key)
    logger.info("Deleted credentials for %s", dag_run_id)
