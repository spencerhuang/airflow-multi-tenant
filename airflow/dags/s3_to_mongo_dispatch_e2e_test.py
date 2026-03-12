"""Temporary dispatcher DAG for cron scheduling e2e test. Auto-generated."""
from datetime import datetime
from airflow.sdk import DAG
from operators.dispatch_operators import DispatchScheduledIntegrationsTask

with DAG(
    dag_id="s3_to_mongo_dispatch_e2e_test",
    description="Temporary dispatcher DAG for cron scheduling e2e test",
    schedule="* * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["test", "e2e", "cron", "dispatcher"],
) as dag:
    dispatch = DispatchScheduledIntegrationsTask(
        task_id="dispatch_integrations",
        schedule_hour=23,
        integration_type="s3_to_mongo",
    )
