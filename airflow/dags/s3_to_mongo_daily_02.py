"""
S3 to MongoDB Daily DAG - Scheduled for 02:00 UTC.

This DAG runs once per day at 02:00 UTC. Instead of executing the
pipeline directly (which would fail because scheduled runs have
conf={}), it acts as a dispatcher: queries the control plane DB for
active integrations scheduled at this hour and triggers
s3_to_mongo_ondemand for each one.

Each integration gets its own isolated DAG run with proper
IntegrationRun tracking.

Configuration:
    All settings read from environment variables (via Kubernetes ConfigMap).
    See config.airflow_config for available environment variables.
"""

from datetime import datetime
from airflow.sdk import DAG

from operators.dispatch_operators import DispatchScheduledIntegrationsTask
from config.airflow_config import get_dag_config, get_default_args

# Get configuration from environment variables
dag_config = get_dag_config()
default_args = get_default_args()

# DAG definition
with DAG(
    dag_id="s3_to_mongo_daily_02",
    default_args=default_args,
    description="Dispatch S3 to MongoDB integrations scheduled at 02:00 UTC",
    schedule="0 2 * * *",  # Runs at 02:00 UTC daily
    start_date=datetime(
        dag_config.start_date_year,
        dag_config.start_date_month,
        dag_config.start_date_day,
    ),
    catchup=dag_config.catchup,
    tags=["s3", "mongodb", "daily", "dispatcher"],
    max_active_runs=1,  # Only one dispatcher run at a time
) as dag:

    dispatch = DispatchScheduledIntegrationsTask(
        task_id="dispatch_integrations",
        schedule_type="daily",
        schedule_hour=2,
        integration_type="s3_to_mongo",
        doc_md="""
        ### Dispatch Integrations
        Queries control plane DB for active S3→MongoDB integrations
        scheduled at 02:00 UTC, then triggers s3_to_mongo_ondemand
        for each one.
        """,
    )
