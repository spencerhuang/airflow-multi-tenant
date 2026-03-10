"""
S3 to MongoDB On-Demand DAG.

This DAG is triggered manually via the Airflow REST API by the control plane service.
Used for:
- CDC-triggered runs
- Manual replays
- Backfills
- Weekly/monthly schedules (triggered by external scheduler)

Configuration:
    All settings read from environment variables (via Kubernetes ConfigMap).
    See config.airflow_config for available environment variables.
"""

from datetime import datetime
from airflow.sdk import DAG
from airflow.utils.trigger_rule import TriggerRule

# Import operators from plugins directory (Airflow automatically adds plugins to path)
from operators.s3_to_mongo_operators import (
    PrepareS3ToMongoTask,
    ValidateS3ToMongoTask,
    ExecuteS3ToMongoTask,
    CleanUpS3ToMongoTask,
)

# Import centralized configuration
from config.airflow_config import get_dag_config, get_default_args

# Get configuration from environment variables
dag_config = get_dag_config()
default_args = get_default_args()

# DAG definition
with DAG(
    dag_id="s3_to_mongo_ondemand",
    default_args=default_args,
    description="S3 to MongoDB on-demand workflow (triggered via API)",
    schedule=None,  # No schedule - triggered externally
    start_date=datetime(
        dag_config.start_date_year,
        dag_config.start_date_month,
        dag_config.start_date_day,
    ),
    catchup=dag_config.catchup,
    tags=["s3", "mongodb", "ondemand", "etl"],
    max_active_runs=dag_config.max_active_runs_ondemand,  # From env: AIRFLOW_MAX_ACTIVE_RUNS_ONDEMAND (default: 50)
    max_active_tasks=dag_config.max_active_tasks,  # From env: AIRFLOW_MAX_ACTIVE_TASKS (default: 5)
) as dag:

    # Task 1: Prepare
    prepare = PrepareS3ToMongoTask(
        task_id="prepare",
        doc_md="""
        ### Prepare Task
        Resolves S3 bucket, prefix, and MongoDB configuration from dag_run.conf.
        Validates required parameters and pushes config to XCom.

        Expected configuration:
        ```json
        {
            "integration_id": 123,
            "tenant_id": "workspace-abc",
            "s3_bucket": "my-bucket",
            "s3_prefix": "data/",
            "mongo_collection": "my_collection"
        }
        ```
        """,
    )

    # Task 2: Validate
    validate = ValidateS3ToMongoTask(
        task_id="validate",
        doc_md="""
        ### Validate Task
        Validates S3 bucket accessibility and MongoDB connection.
        Checks if source data exists.
        """,
    )

    # Task 3: Execute
    execute = ExecuteS3ToMongoTask(
        task_id="execute",
        doc_md="""
        ### Execute Task
        Reads data from S3 and writes to MongoDB.
        Handles data transformation and batch writing.
        Returns statistics on records processed.
        """,
    )

    # Task 4: Cleanup (runs even if upstream tasks fail)
    cleanup = CleanUpS3ToMongoTask(
        task_id="cleanup",
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md="""
        ### Cleanup Task
        Clears sensitive XCom data and updates integration run status.
        Runs even if upstream tasks fail (ALL_DONE trigger rule).
        """,
    )

    # Define task dependencies
    prepare >> validate >> execute >> cleanup
