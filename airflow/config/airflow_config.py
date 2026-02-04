"""
Airflow Configuration Module - Kubernetes Ready

This module provides centralized configuration for Airflow DAGs and tasks,
reading from environment variables with sensible fallback defaults.

Environment variables can be supplied via Kubernetes ConfigMaps.

Usage:
    from config.airflow_config import get_dag_config, get_default_args

    default_args = get_default_args()
    dag_config = get_dag_config()

    with DAG(
        dag_id="my_dag",
        default_args=default_args,
        max_active_runs=dag_config.max_active_runs,
        max_active_tasks=dag_config.max_active_tasks,
    ):
        ...
"""

import os
from datetime import timedelta
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class DAGConfig:
    """
    DAG-level configuration.

    All values read from environment variables with fallback defaults.
    """

    # Concurrency settings
    max_active_runs: int
    max_active_tasks: int
    max_active_runs_ondemand: int  # Higher limit for on-demand DAGs

    # Schedule settings
    catchup: bool
    start_date_year: int
    start_date_month: int
    start_date_day: int

    # Retry settings
    retries: int
    retry_delay_minutes: int
    retry_exponential_backoff: bool
    max_retry_delay_minutes: int

    # Task settings
    depends_on_past: bool
    email_on_failure: bool
    email_on_retry: bool
    execution_timeout_minutes: Optional[int]

    # Sensor settings (if sensors are used)
    sensor_poke_interval_seconds: int
    sensor_timeout_seconds: int
    sensor_mode: str  # 'reschedule' or 'poke'

    @classmethod
    def from_env(cls) -> "DAGConfig":
        """
        Create DAGConfig from environment variables.

        Environment variables:
            AIRFLOW_MAX_ACTIVE_RUNS (default: 10)
            AIRFLOW_MAX_ACTIVE_TASKS (default: 5)
            AIRFLOW_MAX_ACTIVE_RUNS_ONDEMAND (default: 50)
            AIRFLOW_CATCHUP (default: False)
            AIRFLOW_START_DATE_YEAR (default: 2024)
            AIRFLOW_START_DATE_MONTH (default: 1)
            AIRFLOW_START_DATE_DAY (default: 1)
            AIRFLOW_RETRIES (default: 3)
            AIRFLOW_RETRY_DELAY_MINUTES (default: 1)
            AIRFLOW_RETRY_EXPONENTIAL_BACKOFF (default: True)
            AIRFLOW_MAX_RETRY_DELAY_MINUTES (default: 15)
            AIRFLOW_DEPENDS_ON_PAST (default: False)
            AIRFLOW_EMAIL_ON_FAILURE (default: False)
            AIRFLOW_EMAIL_ON_RETRY (default: False)
            AIRFLOW_EXECUTION_TIMEOUT_MINUTES (default: None)
            AIRFLOW_SENSOR_POKE_INTERVAL_SECONDS (default: 60)
            AIRFLOW_SENSOR_TIMEOUT_SECONDS (default: 3600)
            AIRFLOW_SENSOR_MODE (default: reschedule)

        Returns:
            DAGConfig instance with values from environment or defaults
        """
        return cls(
            # Concurrency settings
            max_active_runs=int(os.getenv("AIRFLOW_MAX_ACTIVE_RUNS", "10")),
            max_active_tasks=int(os.getenv("AIRFLOW_MAX_ACTIVE_TASKS", "5")),
            max_active_runs_ondemand=int(
                os.getenv("AIRFLOW_MAX_ACTIVE_RUNS_ONDEMAND", "50")
            ),
            # Schedule settings
            catchup=os.getenv("AIRFLOW_CATCHUP", "False").lower() == "true",
            start_date_year=int(os.getenv("AIRFLOW_START_DATE_YEAR", "2024")),
            start_date_month=int(os.getenv("AIRFLOW_START_DATE_MONTH", "1")),
            start_date_day=int(os.getenv("AIRFLOW_START_DATE_DAY", "1")),
            # Retry settings
            retries=int(os.getenv("AIRFLOW_RETRIES", "3")),
            retry_delay_minutes=int(os.getenv("AIRFLOW_RETRY_DELAY_MINUTES", "1")),
            retry_exponential_backoff=os.getenv(
                "AIRFLOW_RETRY_EXPONENTIAL_BACKOFF", "True"
            ).lower()
            == "true",
            max_retry_delay_minutes=int(
                os.getenv("AIRFLOW_MAX_RETRY_DELAY_MINUTES", "15")
            ),
            # Task settings
            depends_on_past=os.getenv("AIRFLOW_DEPENDS_ON_PAST", "False").lower()
            == "true",
            email_on_failure=os.getenv("AIRFLOW_EMAIL_ON_FAILURE", "False").lower()
            == "true",
            email_on_retry=os.getenv("AIRFLOW_EMAIL_ON_RETRY", "False").lower()
            == "true",
            execution_timeout_minutes=int(os.getenv("AIRFLOW_EXECUTION_TIMEOUT_MINUTES"))
            if os.getenv("AIRFLOW_EXECUTION_TIMEOUT_MINUTES")
            else None,
            # Sensor settings
            sensor_poke_interval_seconds=int(
                os.getenv("AIRFLOW_SENSOR_POKE_INTERVAL_SECONDS", "60")
            ),
            sensor_timeout_seconds=int(
                os.getenv("AIRFLOW_SENSOR_TIMEOUT_SECONDS", "3600")
            ),
            sensor_mode=os.getenv("AIRFLOW_SENSOR_MODE", "reschedule"),
        )


@dataclass
class ControlPlaneConfig:
    """
    Control plane integration configuration.

    All values read from environment variables with fallback defaults.
    """

    # Control plane API
    control_plane_api_url: str
    control_plane_api_timeout_seconds: int

    # Backfill settings
    max_backfill_days: int
    max_backfill_runs_per_integration: int
    backfill_batch_size: int
    backfill_batch_delay_seconds: int

    # DST settings
    default_timezone: str

    @classmethod
    def from_env(cls) -> "ControlPlaneConfig":
        """
        Create ControlPlaneConfig from environment variables.

        Environment variables:
            CONTROL_PLANE_API_URL (default: http://control-plane:8000)
            CONTROL_PLANE_API_TIMEOUT_SECONDS (default: 30)
            AIRFLOW_MAX_BACKFILL_DAYS (default: 7)
            AIRFLOW_MAX_BACKFILL_RUNS_PER_INTEGRATION (default: 7)
            AIRFLOW_BACKFILL_BATCH_SIZE (default: 10)
            AIRFLOW_BACKFILL_BATCH_DELAY_SECONDS (default: 5)
            AIRFLOW_DEFAULT_TIMEZONE (default: UTC)

        Returns:
            ControlPlaneConfig instance with values from environment or defaults
        """
        return cls(
            control_plane_api_url=os.getenv(
                "CONTROL_PLANE_API_URL", "http://control-plane:8000"
            ),
            control_plane_api_timeout_seconds=int(
                os.getenv("CONTROL_PLANE_API_TIMEOUT_SECONDS", "30")
            ),
            max_backfill_days=int(os.getenv("AIRFLOW_MAX_BACKFILL_DAYS", "7")),
            max_backfill_runs_per_integration=int(
                os.getenv("AIRFLOW_MAX_BACKFILL_RUNS_PER_INTEGRATION", "7")
            ),
            backfill_batch_size=int(os.getenv("AIRFLOW_BACKFILL_BATCH_SIZE", "10")),
            backfill_batch_delay_seconds=int(
                os.getenv("AIRFLOW_BACKFILL_BATCH_DELAY_SECONDS", "5")
            ),
            default_timezone=os.getenv("AIRFLOW_DEFAULT_TIMEZONE", "UTC"),
        )


# Singleton instances
_dag_config: Optional[DAGConfig] = None
_control_plane_config: Optional[ControlPlaneConfig] = None


def get_dag_config() -> DAGConfig:
    """
    Get DAG configuration (singleton).

    Returns:
        DAGConfig instance
    """
    global _dag_config
    if _dag_config is None:
        _dag_config = DAGConfig.from_env()
    return _dag_config


def get_control_plane_config() -> ControlPlaneConfig:
    """
    Get control plane configuration (singleton).

    Returns:
        ControlPlaneConfig instance
    """
    global _control_plane_config
    if _control_plane_config is None:
        _control_plane_config = ControlPlaneConfig.from_env()
    return _control_plane_config


def get_default_args() -> Dict[str, Any]:
    """
    Get default arguments for DAG tasks.

    Returns dictionary suitable for DAG default_args parameter.

    Usage:
        default_args = get_default_args()
        with DAG(dag_id="my_dag", default_args=default_args):
            ...

    Returns:
        Dictionary with default arguments
    """
    config = get_dag_config()

    default_args = {
        "owner": "airflow",
        "depends_on_past": config.depends_on_past,
        "email_on_failure": config.email_on_failure,
        "email_on_retry": config.email_on_retry,
        "retries": config.retries,
        "retry_delay": timedelta(minutes=config.retry_delay_minutes),
        "retry_exponential_backoff": config.retry_exponential_backoff,
        "max_retry_delay": timedelta(minutes=config.max_retry_delay_minutes),
    }

    # Add execution timeout if configured
    if config.execution_timeout_minutes:
        default_args["execution_timeout"] = timedelta(
            minutes=config.execution_timeout_minutes
        )

    return default_args


def get_sensor_config() -> Dict[str, Any]:
    """
    Get sensor configuration.

    Returns dictionary suitable for sensor parameters.

    Usage:
        sensor_config = get_sensor_config()
        sensor = S3KeySensor(
            task_id='wait',
            mode=sensor_config['mode'],
            poke_interval=sensor_config['poke_interval'],
            timeout=sensor_config['timeout'],
        )

    Returns:
        Dictionary with sensor configuration
    """
    config = get_dag_config()

    return {
        "mode": config.sensor_mode,
        "poke_interval": config.sensor_poke_interval_seconds,
        "timeout": config.sensor_timeout_seconds,
    }


def log_configuration():
    """
    Log current configuration (useful for debugging).

    Call this at DAG initialization to verify configuration.
    """
    import logging

    logger = logging.getLogger(__name__)

    dag_config = get_dag_config()
    control_plane_config = get_control_plane_config()

    logger.info("=== Airflow Configuration (from environment) ===")
    logger.info(f"Max Active Runs (Daily): {dag_config.max_active_runs}")
    logger.info(f"Max Active Tasks: {dag_config.max_active_tasks}")
    logger.info(f"Max Active Runs (On-Demand): {dag_config.max_active_runs_ondemand}")
    logger.info(f"Retries: {dag_config.retries}")
    logger.info(f"Retry Delay: {dag_config.retry_delay_minutes} minutes")
    logger.info(
        f"Exponential Backoff: {dag_config.retry_exponential_backoff}"
    )
    logger.info(f"Max Retry Delay: {dag_config.max_retry_delay_minutes} minutes")
    logger.info(f"Sensor Mode: {dag_config.sensor_mode}")
    logger.info(f"Control Plane API: {control_plane_config.control_plane_api_url}")
    logger.info("=" * 50)
