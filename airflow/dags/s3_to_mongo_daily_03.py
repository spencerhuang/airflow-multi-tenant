"""
S3 to MongoDB Daily DAG - Scheduled for 03:00 UTC.

Dispatcher DAG: queries the control plane DB for active integrations
scheduled at this hour and triggers s3_to_mongo_ondemand for each one.

See docs/DISPATCHER_PATTERN.md for the full pattern description.
"""

from datetime import datetime
from airflow.sdk import DAG

from operators.dispatch_operators import DispatchScheduledIntegrationsTask
from config.airflow_config import get_dag_config, get_default_args

dag_config = get_dag_config()
default_args = get_default_args()

with DAG(
    dag_id="s3_to_mongo_daily_03",
    default_args=default_args,
    description="Dispatch S3 to MongoDB integrations scheduled at 03:00 UTC",
    schedule="0 3 * * *",
    start_date=datetime(
        dag_config.start_date_year,
        dag_config.start_date_month,
        dag_config.start_date_day,
    ),
    catchup=dag_config.catchup,
    tags=["s3", "mongodb", "daily", "dispatcher"],
    max_active_runs=1,
) as dag:

    dispatch = DispatchScheduledIntegrationsTask(
        task_id="dispatch_integrations",
        schedule_type="daily",
        schedule_hour=3,
        integration_type="s3_to_mongo",
    )
