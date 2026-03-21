"""TLS-aware Redis client singleton with transient credential vault helpers.

Environment detection:
- If REDIS_SENTINEL_HOSTS is set -> Sentinel mode (k8s HA, auto-failover)
- If CA cert file exists at REDIS_CA_CERT_PATH -> enable SSL/mTLS (production)
- Otherwise -> plain TCP (developer mode, no certs needed)

Credential vault:
- store_credentials(dag_run_id, creds) -> SET with 30-min TTL (atomic via setex)
- fetch_credentials(dag_run_id) -> GET with retry; raises AirflowFailException on
  genuine expiry, AirflowException on transient Redis failure (retryable)
- delete_credentials(dag_run_id) -> explicit cleanup by CleanUpTask

Error differentiation:
- Redis down (ConnectionError/TimeoutError) -> retry with backoff, then
  AirflowException (Airflow retries the task per retry config)
- Key expired / never stored -> AirflowFailException (task marked FAILED, no retry)
"""

import json
import os
import logging
import time
from typing import Callable, Optional, TypeVar

import redis as redis_lib

from shared_utils.secret_provider import get_infra_secrets

logger = logging.getLogger(__name__)

T = TypeVar("T")

# TLS certificate paths (standard K8s cert-manager locations)
REDIS_CA_CERT_PATH = os.getenv("REDIS_CA_CERT_PATH", "/etc/ssl/redis/ca.crt")
REDIS_CLIENT_CERT_PATH = os.getenv("REDIS_CLIENT_CERT_PATH", "/etc/ssl/redis/tls.crt")
REDIS_CLIENT_KEY_PATH = os.getenv("REDIS_CLIENT_KEY_PATH", "/etc/ssl/redis/tls.key")

# Default TTL for transient credentials (30 minutes)
CREDENTIAL_TTL_SECONDS = int(os.getenv("CREDENTIAL_TTL_SECONDS", "1800"))

# Retry configuration for transient Redis failures
REDIS_MAX_RETRIES = int(os.getenv("REDIS_MAX_RETRIES", "3"))
REDIS_RETRY_BACKOFF_BASE = float(os.getenv("REDIS_RETRY_BACKOFF_BASE", "0.5"))

# Sentinel configuration (k8s HA mode)
# Format: "host1:port1,host2:port2" — empty string means standalone mode
REDIS_SENTINEL_HOSTS = os.getenv("REDIS_SENTINEL_HOSTS", "")
REDIS_SENTINEL_MASTER = os.getenv("REDIS_SENTINEL_MASTER", "mymaster")

# Redis key prefix for credential isolation
_KEY_PREFIX = "airflow:run_"
_KEY_SUFFIX = ":creds"

# Transient error types that warrant retry
_TRANSIENT_ERRORS = (redis_lib.ConnectionError, redis_lib.TimeoutError)

# Singleton
_redis_client: Optional[redis_lib.Redis] = None


def _parse_sentinel_hosts(hosts_str: str) -> list:
    """Parse 'host1:port1,host2:port2' into [(host1, port1), ...].

    Args:
        hosts_str: Comma-separated host:port pairs.

    Returns:
        List of (host, port) tuples.
    """
    result = []
    for entry in hosts_str.split(","):
        entry = entry.strip()
        if not entry:
            continue
        host, sep, port = entry.rpartition(":")
        if sep:
            result.append((host, int(port)))
        else:
            # No colon found — use entire entry as host with default port
            result.append((entry, 26379))
    return result


def _build_tls_kwargs() -> dict:
    """Build TLS kwargs if cert files exist, otherwise return empty dict."""
    if os.path.isfile(REDIS_CA_CERT_PATH):
        return {
            "ssl": True,
            "ssl_ca_certs": REDIS_CA_CERT_PATH,
            "ssl_certfile": REDIS_CLIENT_CERT_PATH,
            "ssl_keyfile": REDIS_CLIENT_KEY_PATH,
        }
    return {}


def _retry_on_transient(
    operation: Callable[[], T],
    description: str,
    max_retries: int = REDIS_MAX_RETRIES,
) -> T:
    """Execute a Redis operation with retry on transient failure.

    Catches ConnectionError and TimeoutError only (not DataError or other
    non-transient errors). Uses exponential backoff between attempts.

    On success: returns the operation result.
    On exhausted retries: raises AirflowException (retryable by Airflow task
    retry config) or ConnectionError (non-Airflow fallback).

    Args:
        operation: Callable that performs the Redis operation.
        description: Human-readable description for log messages.
        max_retries: Maximum number of attempts.

    Returns:
        The result of the operation.
    """
    for attempt in range(max_retries):
        try:
            return operation()
        except _TRANSIENT_ERRORS as exc:
            if attempt == max_retries - 1:
                msg = (
                    f"Redis unavailable after {max_retries} attempts "
                    f"during {description}: {exc}"
                )
                logger.error(msg)
                try:
                    from airflow.exceptions import AirflowException

                    raise AirflowException(msg) from exc
                except ImportError:
                    raise ConnectionError(msg) from exc
            wait = REDIS_RETRY_BACKOFF_BASE * (2 ** attempt)
            logger.warning(
                "Redis transient error during %s (attempt %d/%d), "
                "retrying in %.1fs: %s",
                description,
                attempt + 1,
                max_retries,
                wait,
                exc,
            )
            time.sleep(wait)
    # Unreachable, but satisfies type checker
    raise AssertionError("unreachable")  # pragma: no cover


def get_redis_client() -> redis_lib.Redis:
    """Get a Redis client singleton with adaptive TLS and optional Sentinel.

    Connection modes (checked in order):
    1. If REDIS_SENTINEL_HOSTS is set -> Sentinel mode (auto-failover)
    2. If REDIS_CA_CERT_PATH exists on disk -> standalone with SSL/mTLS
    3. Otherwise -> standalone plain TCP (developer mode)

    Returns:
        A connected redis.Redis instance (or SentinelManagedConnection
        in Sentinel mode, which auto-follows failover).

    Raises:
        redis.ConnectionError: If Redis is unreachable.
    """
    global _redis_client
    if _redis_client is not None:
        return _redis_client

    secrets = get_infra_secrets()
    password = secrets.redis_password
    tls_kwargs = _build_tls_kwargs()

    if REDIS_SENTINEL_HOSTS:
        # Sentinel mode (k8s HA)
        sentinel_hosts = _parse_sentinel_hosts(REDIS_SENTINEL_HOSTS)
        logger.info(
            "Connecting to Redis via Sentinel: %s, master=%s",
            sentinel_hosts,
            REDIS_SENTINEL_MASTER,
        )
        sentinel_kwargs = {"password": password, **tls_kwargs}
        sentinel = redis_lib.sentinel.Sentinel(
            sentinel_hosts,
            sentinel_kwargs=sentinel_kwargs,
        )
        _redis_client = sentinel.master_for(
            REDIS_SENTINEL_MASTER,
            password=password,
            decode_responses=True,
            **tls_kwargs,
        )
    else:
        # Standalone mode
        host = os.getenv("REDIS_HOST", "redis")
        port = int(os.getenv("REDIS_PORT", "6379"))

        if tls_kwargs:
            logger.info("Connecting to Redis at %s:%d with TLS", host, port)
            _redis_client = redis_lib.Redis(
                host=host,
                port=port,
                password=password,
                decode_responses=True,
                **tls_kwargs,
            )
        else:
            logger.info(
                "Connecting to Redis at %s:%d (plain TCP, no TLS)", host, port
            )
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

    Retries on transient Redis failures with exponential backoff.

    Args:
        dag_run_id: Airflow DAG run ID (used as isolation key).
        credentials: Credential dict to store (serialized as JSON).
        ttl: Time-to-live in seconds (default: 1800 = 30 minutes).

    Raises:
        AirflowException: If Redis is unavailable after retries (retryable).
        ConnectionError: If running outside Airflow and Redis is unavailable.
    """
    client = get_redis_client()
    key = _credential_key(dag_run_id)
    payload = json.dumps(credentials)
    _retry_on_transient(
        lambda: client.setex(key, ttl, payload),
        description=f"store_credentials({dag_run_id})",
    )
    logger.info("Stored credentials for %s (TTL=%ds)", dag_run_id, ttl)


def fetch_credentials(dag_run_id: str) -> dict:
    """Fetch credentials from Redis with retry and error differentiation.

    Phase 1: Attempt GET with retry on transient Redis failures.
    Phase 2: If key is genuinely missing (expired/never stored), raise
    non-retryable exception.

    Args:
        dag_run_id: Airflow DAG run ID.

    Returns:
        The credential dict.

    Raises:
        AirflowException: If Redis is unavailable after retries (Airflow
            will retry the task per its retry config).
        AirflowFailException: If credentials expired or were never stored
            (task marked FAILED immediately, needs business re-trigger).
        ConnectionError: Non-Airflow fallback for Redis unavailability.
        RuntimeError: Non-Airflow fallback for missing credentials.
    """
    client = get_redis_client()
    key = _credential_key(dag_run_id)

    # Phase 1: GET with retry for transient failures
    value = _retry_on_transient(
        lambda: client.get(key),
        description=f"fetch_credentials({dag_run_id})",
    )

    # Phase 2: Key genuinely missing -> non-retryable failure
    if value is None:
        msg = (
            "Credentials expired or never stored for dag_run_id="
            f"'{dag_run_id}'. "
            "Confirm with business domain if task should be re-triggered."
        )
        logger.error(msg)
        try:
            from airflow.exceptions import AirflowFailException

            raise AirflowFailException(msg)
        except ImportError:
            raise RuntimeError(msg)

    return json.loads(value)


def delete_credentials(dag_run_id: str) -> None:
    """Delete credentials from Redis (explicit cleanup by CleanUpTask).

    Retries on transient Redis failures with exponential backoff.

    Args:
        dag_run_id: Airflow DAG run ID.

    Raises:
        AirflowException: If Redis is unavailable after retries (retryable).
        ConnectionError: If running outside Airflow and Redis is unavailable.
    """
    client = get_redis_client()
    key = _credential_key(dag_run_id)
    _retry_on_transient(
        lambda: client.delete(key),
        description=f"delete_credentials({dag_run_id})",
    )
    logger.info("Deleted credentials for %s", dag_run_id)
