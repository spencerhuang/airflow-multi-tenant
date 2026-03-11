"""
S3 to MongoDB Weekly DAG - Scheduled for Monday 03:00 UTC.

Dispatcher DAG: queries the control plane DB for all active weekly
S3-to-MongoDB integrations and triggers s3_to_mongo_ondemand for each one.

Users choose "weekly" frequency when creating an integration. The platform
controls when weekly runs execute (Monday 03:00 UTC). No per-user time
selection — all weekly integrations of this type run together.

See docs/DISPATCHER_PATTERN.md for the full pattern description.
"""

from datetime import datetime
from airflow.sdk import DAG

from operators.dispatch_operators import DispatchScheduledIntegrationsTask
from config.airflow_config import get_dag_config, get_default_args

dag_config = get_dag_config()
default_args = get_default_args()

with DAG(
    dag_id="s3_to_mongo_weekly",
    default_args=default_args,
    description="Dispatch all weekly S3 to MongoDB integrations",
    schedule="0 3 * * 1",  # Monday at 03:00 UTC
    start_date=datetime(
        dag_config.start_date_year,
        dag_config.start_date_month,
        dag_config.start_date_day,
    ),
    catchup=dag_config.catchup,
    tags=["s3", "mongodb", "weekly", "dispatcher"],
    max_active_runs=1,
) as dag:

    dispatch = DispatchScheduledIntegrationsTask(
        task_id="dispatch_integrations",
        schedule_type="weekly",
        integration_type="s3_to_mongo",
    )
