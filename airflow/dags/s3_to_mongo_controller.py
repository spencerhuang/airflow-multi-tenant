"""
S3 to MongoDB Controller DAG - Hourly dispatch via Dynamic Task Mapping.

Runs every hour. Queries the control plane DB for ALL integrations
whose utc_next_run <= now (daily, weekly, monthly). For each due
integration, triggers s3_to_mongo_ondemand via TriggerDagRunOperator
with wait_for_completion=False.

Replaces the static dispatcher DAGs (daily_02, daily_03, weekly, monthly).
The controller fires all runs in seconds and finishes — no "hanging parent"
risk. If no integrations are due, the trigger task is skipped gracefully.

Configuration:
    All settings read from environment variables (via Kubernetes ConfigMap).
    See config.airflow_config for available environment variables.
"""

from datetime import datetime
from airflow.sdk import DAG
from airflow.sdk.definitions.decorators import task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from config.airflow_config import get_dag_config, get_default_args

dag_config = get_dag_config()
default_args = get_default_args()

with DAG(
    dag_id="s3_to_mongo_controller",
    default_args=default_args,
    description="Hourly controller: find due integrations and trigger ondemand DAGs via DTM",
    schedule="0 * * * *",
    start_date=datetime(
        dag_config.start_date_year,
        dag_config.start_date_month,
        dag_config.start_date_day,
    ),
    catchup=False,
    tags=["s3", "mongodb", "controller", "dispatcher"],
    max_active_runs=1,
) as dag:

    @task
    def find_due_integrations() -> list[dict]:
        """Phase A: query DB for due integrations, build confs, advance next_run.

        Returns a list of dicts with ``conf`` and ``trigger_run_id`` keys,
        suitable for TriggerDagRunOperator.expand_kwargs().
        """
        from operators.dispatch_operators import find_and_prepare_due_integrations

        return find_and_prepare_due_integrations(integration_type="s3_to_mongo")

    due = find_due_integrations()

    TriggerDagRunOperator.partial(
        task_id="trigger_ondemand",
        trigger_dag_id="s3_to_mongo_ondemand",
        wait_for_completion=False,
        reset_dag_run=False,
    ).expand_kwargs(due)
