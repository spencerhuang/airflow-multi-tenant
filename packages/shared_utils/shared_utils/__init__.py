"""Shared utilities for airflow-multi-tenant."""

from shared_utils.timezone import TimezoneConverter
from shared_utils.airflow_auth import get_airflow_auth_headers
from shared_utils.task_error_tracking import (
    push_task_errors,
    pull_all_task_errors,
    TASK_ERRORS_XCOM_KEY,
    MAX_ERROR_MESSAGE_LENGTH,
    MAX_ERRORS_PER_TASK,
)
from shared_utils.integration_run import create_integration_run
from shared_utils.db import create_control_plane_engine, DEFAULT_DB_CONNECT_TIMEOUT
from shared_utils.trace_context import TraceContext
from shared_utils.dag_trigger import (
    build_integration_conf,
    merge_json_data,
    resolve_auth_credentials_sync,
    determine_dag_id,
    trigger_airflow_dag,
)
from shared_utils.mongo_parser import parse_mongo_uri
from shared_utils.s3_parser import parse_s3_uri
from shared_utils.secret_provider import (
    read_secret,
    get_infra_secrets,
    reset_infra_secrets,
    InfraSecrets,
)

# Redis client is optional — only available when the `redis` package is installed
# (Airflow containers have it; control_plane and kafka_consumer may not).
try:
    from shared_utils.redis_client import (
        get_redis_client,
        reset_redis_client,
        store_credentials,
        fetch_credentials,
        delete_credentials,
        CREDENTIAL_TTL_SECONDS,
    )
except ImportError:
    pass

__all__ = [
    "TimezoneConverter",
    "get_airflow_auth_headers",
    "push_task_errors",
    "pull_all_task_errors",
    "TASK_ERRORS_XCOM_KEY",
    "MAX_ERROR_MESSAGE_LENGTH",
    "MAX_ERRORS_PER_TASK",
    "create_integration_run",
    "create_control_plane_engine",
    "DEFAULT_DB_CONNECT_TIMEOUT",
    "TraceContext",
    "build_integration_conf",
    "merge_json_data",
    "resolve_auth_credentials_sync",
    "determine_dag_id",
    "trigger_airflow_dag",
    "parse_mongo_uri",
    "parse_s3_uri",
    "read_secret",
    "get_infra_secrets",
    "reset_infra_secrets",
    "InfraSecrets",
    "get_redis_client",
    "reset_redis_client",
    "store_credentials",
    "fetch_credentials",
    "delete_credentials",
    "CREDENTIAL_TTL_SECONDS",
]
