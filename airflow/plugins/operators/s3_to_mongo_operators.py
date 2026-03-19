"""S3 to MongoDB workflow operators."""

import json
import os
from typing import Dict, Any, List
from datetime import datetime, timezone

from airflow.sdk import BaseOperator
from airflow.models import XCom
from sqlalchemy import select

# Use relative import within plugins directory
from operators.base_operators import PrepareTask, ValidateTask, CleanUpTask, TraceIdMixin

# Import reusable connectors
from connectors.s3.auth import S3Auth
from connectors.s3.client import S3Client
from connectors.s3.reader import S3Reader
from connectors.mongo.auth import MongoAuth
from connectors.mongo.client import MongoClient

# Shared Core table definitions for the control-plane database
from shared_models.tables import (
    integration_runs as integration_runs_table,
    integration_run_errors as integration_run_errors_table,
)

# Shared utilities
from shared_utils import (
    push_task_errors,
    pull_all_task_errors,
    create_integration_run,
    create_control_plane_engine,
    parse_mongo_uri,
)


class PrepareS3ToMongoTask(PrepareTask):
    """
    Prepare task for S3 to MongoDB workflow.

    Resolves:
    - S3 bucket and prefix
    - MongoDB connection details
    - IAM role or credentials
    - Creates IntegrationRun record in control plane DB
    """

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute S3 to MongoDB preparation.

        Extracts non-sensitive config (returned via default XCom) and
        credentials (pushed to a separate 'credentials' XCom key).
        Creates an IntegrationRun record so CleanUp can update it later.

        Args:
            context: Airflow task context with dag_run.conf

        Returns:
            Configuration dictionary for downstream tasks (no secrets)
        """
        dag_run = context.get("dag_run")
        dag_run_conf = (dag_run.conf if dag_run else None) or {}
        ti = context["ti"]

        trace_ctx = self._get_trace_context(context)
        trace_id = trace_ctx.trace_id
        dag_run_id = dag_run.run_id if dag_run else "unknown"
        ti.xcom_push(key="traceparent", value=trace_ctx.traceparent)

        integration_id = dag_run_conf.get("integration_id")
        self.log.info(f"[trace_id={trace_id}][dag_run_id={dag_run_id}] Preparing S3 to MongoDB integration: {integration_id}")

        # Create IntegrationRun record FIRST — must exist before any logic
        # that could fail, so CleanUp always has a row to update.
        create_integration_run(integration_id, dag_run.run_id, log=self.log)

        try:
            # Extract configuration
            s3_bucket = dag_run_conf.get("s3_bucket")
            s3_prefix = dag_run_conf.get("s3_prefix", "")
            mongo_collection = dag_run_conf.get("mongo_collection")

            # Validate required parameters
            if not s3_bucket:
                raise ValueError("s3_bucket is required in configuration")
            if not mongo_collection:
                raise ValueError("mongo_collection is required in configuration")

            self.log.info(f"[trace_id={trace_id}] S3 bucket: {s3_bucket}, prefix: {s3_prefix}")
            self.log.info(f"[trace_id={trace_id}] MongoDB collection: {mongo_collection}")

            # Push credentials to a separate XCom key so downstream tasks
            # can use them without exposing secrets in the default return value.
            credentials = {
                "s3_endpoint_url": dag_run_conf.get("s3_endpoint_url", "http://minio:9000"),
                "s3_access_key": dag_run_conf.get("s3_access_key", "minioadmin"),
                "s3_secret_key": dag_run_conf.get("s3_secret_key", "minioadmin"),
                "mongo_uri": dag_run_conf.get("mongo_uri", "mongodb://root:root@mongodb:27017/"),
                "mongo_database": dag_run_conf.get("mongo_database", "test_database"),
            }
            ti.xcom_push(key="credentials", value=credentials)
            self.log.info(f"[trace_id={trace_id}] Credentials pushed to XCom (separate key)")

            # Return non-sensitive config (default XCom)
            return {
                "s3_bucket": s3_bucket,
                "s3_prefix": s3_prefix,
                "mongo_collection": mongo_collection,
                "integration_id": integration_id,
            }

        except Exception as e:
            # Push error to XCom so CleanUp can persist it
            self.log.error(f"[trace_id={trace_id}][dag_run_id={dag_run_id}] Prepare failed: {e}", exc_info=True)
            push_task_errors(ti, "prepare", [
                {"error_code": "PREPARE_ERROR", "message": f"Prepare failed: {e}"},
            ], log=self.log)
            raise

class ValidateS3ToMongoTask(ValidateTask):
    """
    Validate task for S3 to MongoDB workflow.

    Validates:
    - S3 objects exist
    - MongoDB connection is accessible
    """

    def execute(self, context: Dict[str, Any]) -> bool:
        """
        Execute S3 to MongoDB validation using reusable connectors.

        Args:
            context: Airflow task context

        Returns:
            True if validation passes

        Raises:
            Exception: If validation fails
        """
        # Pull configuration and credentials from XCom
        ti = context["ti"]
        trace_id = self._get_trace_context(context).trace_id
        dag_run_id = context.get("dag_run").run_id if context.get("dag_run") else "unknown"
        config = ti.xcom_pull(task_ids="prepare")
        credentials = ti.xcom_pull(task_ids="prepare", key="credentials")

        self.log.info(f"[trace_id={trace_id}][dag_run_id={dag_run_id}] Validating S3 to MongoDB configuration: {config}")

        s3_bucket = config["s3_bucket"]
        s3_prefix = config.get("s3_prefix", "")

        # Get credentials from XCom (pushed by prepare task)
        s3_endpoint = credentials.get("s3_endpoint_url", "http://minio:9000")
        s3_access_key = credentials.get("s3_access_key", "minioadmin")
        s3_secret_key = credentials.get("s3_secret_key", "minioadmin")
        mongo_uri = credentials.get("mongo_uri", "mongodb://root:root@mongodb:27017/")
        mongo_database = credentials.get("mongo_database", "test_database")

        try:
            # 1. Validate S3 bucket is accessible using connector
            self.log.info(f"[trace_id={trace_id}] Validating S3 bucket {s3_bucket} with prefix {s3_prefix}")
            s3_auth = S3Auth(
                aws_access_key_id=s3_access_key,
                aws_secret_access_key=s3_secret_key,
                region_name='us-east-1',
                endpoint_url=s3_endpoint,
            )
            s3_client = S3Client(s3_auth)

            # Try to list objects to verify access
            objects = s3_client.list_objects(s3_bucket, s3_prefix, max_keys=1)
            self.log.info(f"[trace_id={trace_id}] S3 bucket accessible, found objects: {len(objects) > 0}")

            # 2. Validate MongoDB connection using connector
            self.log.info(f"[trace_id={trace_id}] Validating MongoDB connection")
            parsed = parse_mongo_uri(mongo_uri)
            parsed["database"] = mongo_database  # config override
            mongo_auth = MongoAuth.from_dict(parsed)
            mongo_client = MongoClient(mongo_auth)

            # Test connection by listing collections
            mongo_client.db.list_collection_names()
            mongo_client.close()
            self.log.info(f"[trace_id={trace_id}] MongoDB connection successful")

            self.log.info(f"[trace_id={trace_id}] Validation successful")
            return True

        except Exception as e:
            error_msg = f"Validation failed: {str(e)}"
            self.log.error(f"[trace_id={trace_id}][dag_run_id={dag_run_id}] {error_msg}", exc_info=True)
            push_task_errors(ti, "validate", [
                {"error_code": "VALIDATION_ERROR", "message": error_msg},
            ], log=self.log)
            raise Exception(error_msg)


class ExecuteS3ToMongoTask(TraceIdMixin, BaseOperator):
    """
    Execute task for S3 to MongoDB workflow.

    Executes:
    - Reads data from S3
    - Transforms data if needed
    - Writes data to MongoDB
    """

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute S3 to MongoDB data transfer using reusable connectors.

        Args:
            context: Airflow task context

        Returns:
            Execution statistics
        """
        # Pull configuration and credentials from XCom
        ti = context["ti"]
        trace_id = self._get_trace_context(context).trace_id
        dag_run_id = context.get("dag_run").run_id if context.get("dag_run") else "unknown"
        config = ti.xcom_pull(task_ids="prepare")
        credentials = ti.xcom_pull(task_ids="prepare", key="credentials")

        self.log.info(f"[trace_id={trace_id}][dag_run_id={dag_run_id}] Executing S3 to MongoDB transfer: {config}")

        s3_bucket = config["s3_bucket"]
        s3_prefix = config.get("s3_prefix", "")
        mongo_collection = config["mongo_collection"]

        # Get credentials from XCom (pushed by prepare task)
        s3_endpoint = credentials.get("s3_endpoint_url", "http://minio:9000")
        s3_access_key = credentials.get("s3_access_key", "minioadmin")
        s3_secret_key = credentials.get("s3_secret_key", "minioadmin")
        mongo_uri = credentials.get("mongo_uri", "mongodb://root:root@mongodb:27017/")
        mongo_database = credentials.get("mongo_database", "test_database")

        stats = {
            "files_processed": 0,
            "records_read": 0,
            "records_written": 0,
            "errors": 0,
            "error_messages": [],
        }

        try:
            # 1. Initialize S3 connector (reusable)
            self.log.info(f"Connecting to S3 using connector: {s3_endpoint}")
            s3_auth = S3Auth(
                aws_access_key_id=s3_access_key,
                aws_secret_access_key=s3_secret_key,
                region_name='us-east-1',
                endpoint_url=s3_endpoint,
            )
            s3_client = S3Client(s3_auth)
            s3_reader = S3Reader(s3_client)

            # 2. Initialize MongoDB connector (reusable)
            parsed = parse_mongo_uri(mongo_uri)
            parsed["database"] = mongo_database  # config override
            self.log.info(f"Connecting to MongoDB using connector: {parsed['host']}:{parsed['port']}")
            mongo_auth = MongoAuth.from_dict(parsed)
            mongo_client = MongoClient(mongo_auth)

            # 3. List S3 objects using connector
            self.log.info(f"Listing objects in s3://{s3_bucket}/{s3_prefix}")
            objects = s3_client.list_objects(s3_bucket, s3_prefix, max_keys=1000)

            if not objects:
                self.log.info(f"No objects found in s3://{s3_bucket}/{s3_prefix}")
                return stats

            # 4. Process each file using S3Reader
            for obj in objects:
                key = obj['Key']

                # Skip directories
                if key.endswith('/'):
                    continue

                # Only process JSON files
                if not key.endswith('.json'):
                    self.log.info(f"Skipping non-JSON file: {key}")
                    continue

                try:
                    self.log.info(f"Processing: {key}")

                    # Read JSON file using S3Reader (reusable)
                    data = s3_reader.read_json(s3_bucket, key)

                    # Handle both single objects and arrays
                    if isinstance(data, dict):
                        records = [data]
                    elif isinstance(data, list):
                        records = data
                    else:
                        self.log.warning(f"Unexpected data type in {key}: {type(data)}")
                        stats["errors"] += 1
                        continue

                    stats["records_read"] += len(records)

                    # Add metadata to each record
                    for record in records:
                        record['_import_timestamp'] = datetime.now(timezone.utc)
                        record['_source_bucket'] = s3_bucket
                        record['_source_key'] = key

                    # Insert into MongoDB using connector (reusable)
                    if records:
                        inserted_ids = mongo_client.insert_many(mongo_collection, records)
                        inserted_count = len(inserted_ids)
                        stats["records_written"] += inserted_count
                        self.log.info(f"✓ Inserted {inserted_count} records from {key}")

                    stats["files_processed"] += 1

                except json.JSONDecodeError as e:
                    error_msg = f"JSON parse error in {key}: {str(e)}"
                    self.log.error(f"[trace_id={trace_id}][dag_run_id={dag_run_id}] {error_msg}")
                    stats["errors"] += 1
                    stats["error_messages"].append(error_msg)

                except Exception as e:
                    error_msg = f"Error processing {key}: {str(e)}"
                    self.log.error(f"[trace_id={trace_id}][dag_run_id={dag_run_id}] {error_msg}", exc_info=True)
                    stats["errors"] += 1
                    stats["error_messages"].append(error_msg)

            # 5. Close connections
            mongo_client.close()

            # Push data-level errors to XCom for CleanUp to persist
            if stats["error_messages"]:
                push_task_errors(ti, "execute", [
                    {"error_code": "DATA_ERROR", "message": msg}
                    for msg in stats["error_messages"]
                ], log=self.log)

            self.log.info(f"[trace_id={trace_id}] Transfer complete: {stats}")
            return stats

        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            self.log.error(f"[trace_id={trace_id}][dag_run_id={dag_run_id}] {error_msg}", exc_info=True)
            stats["errors"] += 1
            stats["error_messages"].append(error_msg)
            # Push all accumulated errors to XCom before re-raising
            push_task_errors(ti, "execute", [
                {"error_code": "EXECUTE_ERROR", "message": msg}
                for msg in stats["error_messages"]
            ], log=self.log)
            raise


class CleanUpS3ToMongoTask(CleanUpTask):
    """
    Cleanup task for S3 to MongoDB workflow.

    Runs with trigger_rule=ALL_DONE so it executes even when upstream tasks fail.

    Performs:
    1. Closes S3 and MongoDB connections (per-task in Airflow; acknowledged here)
    2. Clears sensitive credential data from XCom
    3. Updates IntegrationRun status in control plane database
    """

    def execute(self, context: Dict[str, Any]) -> None:
        """
        Execute S3 to MongoDB cleanup.

        Args:
            context: Airflow task context
        """
        ti = context["ti"]
        dag_run = context.get("dag_run")
        trace_id = self._get_trace_context(context).trace_id

        # Pull XCom data — may be None if upstream tasks failed
        config = ti.xcom_pull(task_ids="prepare")
        stats = ti.xcom_pull(task_ids="execute")

        # Resolve integration_id: XCom config first, fallback to dag_run.conf
        integration_id = None
        if config and isinstance(config, dict):
            integration_id = config.get("integration_id")
        if integration_id is None:
            dag_run_conf = dag_run.conf or {}
            integration_id = dag_run_conf.get("integration_id")

        self.log.info(f"[trace_id={trace_id}] Cleaning up S3 to MongoDB integration: {integration_id}")
        if stats:
            self.log.info(f"[trace_id={trace_id}] Final statistics: {stats}")

        # 1. Close S3 and MongoDB connections
        # In Airflow, each task runs in its own process. Connections created
        # in validate/execute are already closed when those tasks finish.
        # No shared connections to close here.
        self.log.info(f"[trace_id={trace_id}] Step 1: Connections are per-task; no shared connections to close")

        dag_run_id = dag_run.run_id

        # 2. Clear sensitive credential data from XCom
        self._clear_sensitive_xcom(context, trace_id, dag_run_id)

        # 3. Update IntegrationRun status in control plane database
        self._update_integration_run_status(context, integration_id, trace_id, dag_run_id)

        self.log.info(f"[trace_id={trace_id}] Cleanup complete")

    def _clear_sensitive_xcom(self, context: Dict[str, Any], trace_id: str, dag_run_id: str) -> None:
        """Delete the 'credentials' XCom key pushed by PrepareTask."""
        try:
            dag_run = context.get("dag_run")
            ti = context["ti"]
            session = ti.get_session()

            session.query(XCom).filter(
                XCom.dag_id == dag_run.dag_id,
                XCom.task_id == "prepare",
                XCom.run_id == dag_run.run_id,
                XCom.key == "credentials",
            ).delete()
            session.commit()
            self.log.info(f"[trace_id={trace_id}][dag_run_id={dag_run_id}] Step 2: Cleared sensitive 'credentials' XCom data")
        except Exception as e:
            self.log.warning(f"[trace_id={trace_id}][dag_run_id={dag_run_id}] Step 2: Could not clear XCom credentials: {e}")

    def _update_integration_run_status(
        self,
        context: Dict[str, Any],
        integration_id: Any,
        trace_id: str,
        dag_run_id: str,
    ) -> None:
        """Update IntegrationRun in control plane DB with final status and errors."""
        if integration_id is None:
            self.log.warning(f"[trace_id={trace_id}][dag_run_id={dag_run_id}] Step 3: No integration_id available; skipping DB status update")
            return

        db_url = os.environ.get("CONTROL_PLANE_DB_URL")
        if not db_url:
            self.log.warning(f"[trace_id={trace_id}][dag_run_id={dag_run_id}] Step 3: CONTROL_PLANE_DB_URL not set; skipping DB status update")
            return

        try:
            ti = context["ti"]
            dag_run = context.get("dag_run")
            dag_run_id = dag_run.run_id

            # Collect errors from XCom (detailed messages pushed by upstream tasks)
            upstream_task_ids = ["prepare", "validate", "execute"]
            upstream_errors = pull_all_task_errors(ti, upstream_task_ids)

            # Fallback: check task states for tasks that failed without pushing errors
            state_errors = self._collect_upstream_state_errors(context, upstream_errors)
            upstream_errors.extend(state_errors)

            is_success = len(upstream_errors) == 0

            engine = create_control_plane_engine(db_url)

            with engine.connect() as conn:
                # Find the IntegrationRun record by dag_run_id
                result = conn.execute(
                    select(integration_runs_table.c.run_id)
                    .where(integration_runs_table.c.dag_run_id == dag_run_id)
                    .limit(1)
                )
                row = result.fetchone()

                if row is None:
                    self.log.info(
                        f"Step 3: No IntegrationRun found for dag_run_id={dag_run_id}. "
                        "This is normal when integration_id is absent from conf."
                    )
                    engine.dispose()
                    return

                run_id = row[0]

                # Update IntegrationRun with final status
                conn.execute(
                    integration_runs_table.update()
                    .where(integration_runs_table.c.run_id == run_id)
                    .values(
                        ended=datetime.now(timezone.utc),
                        is_success=is_success,
                    )
                )

                # Insert error records
                for error in upstream_errors:
                    conn.execute(
                        integration_run_errors_table.insert().values(
                            run_id=run_id,
                            error_code=error["error_code"],
                            message=str(error["message"])[:2000],
                            task_id=error["task_id"],
                            timestamp=datetime.now(timezone.utc),
                        )
                    )

                conn.commit()
                self.log.info(
                    f"Step 3: Updated IntegrationRun run_id={run_id}: "
                    f"is_success={is_success}, errors={len(upstream_errors)}"
                )

            engine.dispose()

        except Exception as e:
            self.log.error(f"[trace_id={trace_id}][dag_run_id={dag_run_id}] Step 3: Failed to update IntegrationRun status: {e}")

    def _collect_upstream_state_errors(
        self,
        context: Dict[str, Any],
        xcom_errors: List[Dict[str, str]],
    ) -> List[Dict[str, str]]:
        """Fallback: check task states for tasks that failed but didn't push XCom errors.

        Only adds a generic state-based error if the task_id isn't already
        represented in xcom_errors (avoids duplicates).
        """
        errors = []
        upstream_task_ids = ["prepare", "validate", "execute"]

        # task_ids already covered by XCom errors
        covered_task_ids = {e["task_id"] for e in xcom_errors}

        try:
            dag_run = context.get("dag_run")
            task_instances = dag_run.get_task_instances()

            for task_instance in task_instances:
                if task_instance.task_id in upstream_task_ids:
                    if task_instance.task_id not in covered_task_ids:
                        if task_instance.state in ("failed", "upstream_failed"):
                            errors.append({
                                "task_id": task_instance.task_id,
                                "error_code": f"TASK_{task_instance.state.upper()}",
                                "message": (
                                    f"Task '{task_instance.task_id}' ended in state "
                                    f"'{task_instance.state}'"
                                ),
                            })
        except Exception as e:
            trace_id = self._get_trace_context(context).trace_id
            dag_run_id = context.get("dag_run").run_id if context.get("dag_run") else "unknown"
            self.log.warning(f"[trace_id={trace_id}][dag_run_id={dag_run_id}] Could not check upstream task states: {e}")
            errors.append({
                "task_id": "cleanup",
                "error_code": "STATE_CHECK_ERROR",
                "message": f"Failed to check upstream states: {e}",
            })

        return errors
