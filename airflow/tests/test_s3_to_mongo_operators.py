"""Unit tests for S3 to MongoDB operators.

These tests mock the Airflow framework dependencies so they can run
without Apache Airflow installed (e.g., in the host dev environment).
"""

import pytest
import os
import sys
from unittest.mock import Mock, MagicMock, patch, call
from types import ModuleType
from datetime import datetime


# ---------------------------------------------------------------------------
# Mock Airflow modules so operators can be imported without Airflow installed
# ---------------------------------------------------------------------------
def _setup_airflow_mocks():
    """Create mock Airflow modules and inject them into sys.modules.

    Airflow 3.0 moved BaseOperator, DAG, TriggerRule to airflow.sdk.
    XCom remains in airflow.models.
    """
    # Create a real-looking BaseOperator mock class
    class MockBaseOperator:
        def __init__(self, *args, **kwargs):
            self.task_id = kwargs.get("task_id", "")
            self.trigger_rule = kwargs.get("trigger_rule", "all_success")
            self.doc_md = kwargs.get("doc_md", "")

        @property
        def log(self):
            import logging
            return logging.getLogger(f"airflow.task.{self.task_id}")

    # Mock XCom model
    class MockXCom:
        dag_id = "dag_id"
        task_id = "task_id"
        run_id = "run_id"
        key = "key"

    # TriggerRule enum mock
    class TriggerRule:
        ALL_SUCCESS = "all_success"
        ALL_DONE = "all_done"

    # Build mock module tree
    airflow_mod = ModuleType("airflow")
    airflow_sdk = ModuleType("airflow.sdk")
    airflow_models = ModuleType("airflow.models")

    # Airflow 3: BaseOperator and TriggerRule live in airflow.sdk
    airflow_sdk.BaseOperator = MockBaseOperator
    airflow_sdk.TriggerRule = TriggerRule

    # XCom remains in airflow.models
    airflow_models.XCom = MockXCom

    sys.modules["airflow"] = airflow_mod
    sys.modules["airflow.sdk"] = airflow_sdk
    sys.modules["airflow.models"] = airflow_models

    return MockXCom


# Check if real Airflow is available; if not, use mocks
try:
    from airflow.sdk import BaseOperator  # noqa: F401
    _MockXCom = None
except (ImportError, ModuleNotFoundError):
    _MockXCom = _setup_airflow_mocks()

# Now add plugins to path and import operators
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../plugins"))

from operators.s3_to_mongo_operators import (
    PrepareS3ToMongoTask,
    ValidateS3ToMongoTask,
    ExecuteS3ToMongoTask,
    CleanUpS3ToMongoTask,
)
from operators.dispatch_operators import DispatchScheduledIntegrationsTask


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_prepare_context(conf, run_id="test_run_123"):
    """Build a mock context for Prepare task tests."""
    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = None
    mock_dag_run = Mock()
    mock_dag_run.conf = conf
    mock_dag_run.run_id = run_id
    return {"dag_run": mock_dag_run, "ti": mock_ti}


class TestPrepareS3ToMongoTask:
    """Test PrepareS3ToMongoTask operator."""

    @patch("operators.s3_to_mongo_operators.store_credentials")
    def test_execute_success(self, mock_store_creds):
        """Test successful execution stores credentials in Redis, not XCom."""
        operator = PrepareS3ToMongoTask(task_id="prepare")

        context = _make_prepare_context({
            "integration_id": 1,
            "s3_bucket": "test-bucket",
            "s3_prefix": "data/",
            "mongo_collection": "test_collection",
            "s3_endpoint_url": "http://custom-s3:9000",
            "s3_access_key": "mykey",
            "s3_secret_key": "mysecret",
            "mongo_uri": "mongodb://user:pass@host:27017/",
            "mongo_database": "mydb",
        })

        result = operator.execute(context)

        # Config return value should NOT contain credentials
        assert result["s3_bucket"] == "test-bucket"
        assert result["s3_prefix"] == "data/"
        assert result["mongo_collection"] == "test_collection"
        assert result["integration_id"] == 1
        assert "s3_access_key" not in result
        assert "s3_secret_key" not in result
        assert "mongo_uri" not in result

        # Credentials should be stored in Redis (not XCom)
        mock_store_creds.assert_called_once_with(
            "test_run_123",
            {
                "s3_endpoint_url": "http://custom-s3:9000",
                "s3_access_key": "mykey",
                "s3_secret_key": "mysecret",
                "mongo_uri": "mongodb://user:pass@host:27017/",
                "mongo_database": "mydb",
            },
        )

        # Verify no credentials were pushed to XCom
        mock_ti = context["ti"]
        for c in mock_ti.xcom_push.call_args_list:
            assert c[1].get("key") != "credentials"

    @patch("operators.s3_to_mongo_operators.store_credentials")
    def test_execute_credentials_use_defaults(self, mock_store_creds):
        """Test that missing credentials use defaults."""
        operator = PrepareS3ToMongoTask(task_id="prepare")

        context = _make_prepare_context({
            "s3_bucket": "test-bucket",
            "mongo_collection": "test_collection",
        })

        operator.execute(context)

        # Verify defaults are used in credentials stored to Redis
        creds = mock_store_creds.call_args[0][1]
        assert creds["s3_endpoint_url"] == "http://minio:9000"
        assert creds["s3_access_key"] == "minioadmin"
        assert creds["s3_secret_key"] == "minioadmin"
        assert creds["mongo_uri"] == "mongodb://root:root@mongodb:27017/"
        assert creds["mongo_database"] == "test_database"

    def test_execute_missing_s3_bucket(self):
        """Test execution fails when s3_bucket is missing."""
        operator = PrepareS3ToMongoTask(task_id="prepare")

        context = _make_prepare_context({"mongo_collection": "test_collection"})

        with pytest.raises(ValueError, match="s3_bucket is required"):
            operator.execute(context)

        # Verify error was pushed to XCom
        context["ti"].xcom_push.assert_any_call(
            key="task_errors_prepare",
            value=[{
                "task_id": "prepare",
                "error_code": "PREPARE_ERROR",
                "message": "Prepare failed: s3_bucket is required in configuration",
            }],
        )

    def test_execute_missing_mongo_collection(self):
        """Test execution fails when mongo_collection is missing."""
        operator = PrepareS3ToMongoTask(task_id="prepare")

        context = _make_prepare_context({"s3_bucket": "test-bucket"})

        with pytest.raises(ValueError, match="mongo_collection is required"):
            operator.execute(context)

        # Verify error was pushed to XCom
        context["ti"].xcom_push.assert_any_call(
            key="task_errors_prepare",
            value=[{
                "task_id": "prepare",
                "error_code": "PREPARE_ERROR",
                "message": "Prepare failed: mongo_collection is required in configuration",
            }],
        )

    @patch("operators.s3_to_mongo_operators.store_credentials")
    def test_execute_with_default_prefix(self, mock_store_creds):
        """Test execution with default s3_prefix."""
        operator = PrepareS3ToMongoTask(task_id="prepare")

        context = _make_prepare_context({
            "s3_bucket": "test-bucket",
            "mongo_collection": "test_collection",
        })

        result = operator.execute(context)
        assert result["s3_prefix"] == ""

    @patch("operators.s3_to_mongo_operators.store_credentials")
    def test_execute_with_empty_conf(self, mock_store_creds):
        """Test execution with None conf."""
        operator = PrepareS3ToMongoTask(task_id="prepare")

        context = _make_prepare_context(None)

        with pytest.raises(ValueError):
            operator.execute(context)

    @patch("operators.s3_to_mongo_operators.store_credentials")
    @patch("operators.s3_to_mongo_operators.create_integration_run")
    def test_execute_creates_integration_run(self, mock_create_run, mock_store_creds):
        """Test that Prepare calls create_integration_run with correct args."""
        operator = PrepareS3ToMongoTask(task_id="prepare")

        context = _make_prepare_context({
            "integration_id": 42,
            "s3_bucket": "test-bucket",
            "mongo_collection": "test_collection",
        }, run_id="ws1_s3_to_mongo_ondemand_scheduled_20260310_0200")

        result = operator.execute(context)

        assert result["integration_id"] == 42

        # Verify create_integration_run was called with correct args
        mock_create_run.assert_called_once_with(
            42, "ws1_s3_to_mongo_ondemand_scheduled_20260310_0200", log=operator.log,
        )

    @patch("operators.s3_to_mongo_operators.store_credentials")
    def test_execute_skips_integration_run_without_db_url(self, mock_store_creds):
        """Test that Prepare skips IntegrationRun creation when no DB URL."""
        operator = PrepareS3ToMongoTask(task_id="prepare")

        context = _make_prepare_context({
            "integration_id": 1,
            "s3_bucket": "test-bucket",
            "mongo_collection": "test_collection",
        })

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("CONTROL_PLANE_DB_URL", None)
            result = operator.execute(context)

        # Should succeed without DB interaction
        assert result["integration_id"] == 1

    @patch("operators.s3_to_mongo_operators.store_credentials")
    def test_execute_skips_integration_run_without_integration_id(self, mock_store_creds):
        """Test that Prepare skips IntegrationRun creation when no integration_id."""
        operator = PrepareS3ToMongoTask(task_id="prepare")

        context = _make_prepare_context({
            "s3_bucket": "test-bucket",
            "mongo_collection": "test_collection",
        })

        result = operator.execute(context)
        assert result["integration_id"] is None

    @patch("operators.s3_to_mongo_operators.store_credentials")
    @patch("operators.s3_to_mongo_operators.create_integration_run")
    def test_execute_creates_integration_run_even_on_failure(self, mock_create_run, mock_store_creds):
        """Test that IntegrationRun is created even when validation fails."""
        operator = PrepareS3ToMongoTask(task_id="prepare")

        # Has integration_id but missing s3_bucket — validation will fail
        context = _make_prepare_context({
            "integration_id": 42,
            "mongo_collection": "test_collection",
        })

        with pytest.raises(ValueError, match="s3_bucket is required"):
            operator.execute(context)

        # IntegrationRun should still have been created before the error
        mock_create_run.assert_called_once_with(42, "test_run_123", log=operator.log)


class TestValidateS3ToMongoTask:
    """Test ValidateS3ToMongoTask operator."""

    @patch("operators.s3_to_mongo_operators.fetch_credentials")
    @patch("operators.s3_to_mongo_operators.MongoClient")
    @patch("operators.s3_to_mongo_operators.MongoAuth")
    @patch("operators.s3_to_mongo_operators.S3Client")
    @patch("operators.s3_to_mongo_operators.S3Auth")
    def test_execute_fetches_credentials_from_redis(
        self, mock_s3_auth_cls, mock_s3_client_cls, mock_mongo_auth_cls, mock_mongo_client_cls,
        mock_fetch_creds
    ):
        """Test that validate reads credentials from Redis, not XCom."""
        operator = ValidateS3ToMongoTask(task_id="validate")

        mock_s3_client_cls.return_value.list_objects.return_value = [{"Key": "test"}]
        mock_mongo_client_cls.return_value.db.list_collection_names.return_value = []

        config = {
            "s3_bucket": "test-bucket",
            "s3_prefix": "data/",
            "mongo_collection": "test_collection",
        }
        credentials = {
            "s3_endpoint_url": "http://minio:9000",
            "s3_access_key": "mykey",
            "s3_secret_key": "mysecret",
            "mongo_uri": "mongodb://user:pass@host:27017/",
            "mongo_database": "mydb",
        }

        mock_fetch_creds.return_value = credentials

        mock_ti = Mock()
        mock_ti.xcom_pull.return_value = config
        mock_dag_run = Mock()
        mock_dag_run.run_id = "test_run_123"
        mock_dag_run.conf = {}
        context = {"ti": mock_ti, "dag_run": mock_dag_run}

        result = operator.execute(context)
        assert result is True

        # Verify credentials were fetched from Redis
        mock_fetch_creds.assert_called_once_with("test_run_123")

        # Verify S3Auth was created with Redis credentials
        mock_s3_auth_cls.assert_called_once_with(
            aws_access_key_id="mykey",
            aws_secret_access_key="mysecret",
            region_name="us-east-1",
            endpoint_url="http://minio:9000",
        )

    @patch("operators.s3_to_mongo_operators.fetch_credentials")
    @patch("operators.s3_to_mongo_operators.MongoClient")
    @patch("operators.s3_to_mongo_operators.MongoAuth")
    @patch("operators.s3_to_mongo_operators.S3Client")
    @patch("operators.s3_to_mongo_operators.S3Auth")
    def test_execute_validation_failure(
        self, mock_s3_auth_cls, mock_s3_client_cls, mock_mongo_auth_cls, mock_mongo_client_cls,
        mock_fetch_creds
    ):
        """Test that validation failure raises exception and pushes error to XCom."""
        operator = ValidateS3ToMongoTask(task_id="validate")

        mock_s3_client_cls.return_value.list_objects.side_effect = Exception("Access denied")

        mock_fetch_creds.return_value = {
            "s3_endpoint_url": "http://minio:9000",
            "s3_access_key": "bad", "s3_secret_key": "bad",
            "mongo_uri": "mongodb://host:27017/", "mongo_database": "db",
        }

        mock_ti = Mock()
        mock_ti.xcom_pull.side_effect = lambda task_ids=None, key=None: {
            ("prepare", None): {"s3_bucket": "bad-bucket", "s3_prefix": ""},
            ("prepare", "traceparent"): None,
        }.get((task_ids, key))
        mock_dag_run = Mock()
        mock_dag_run.run_id = "test_run_123"
        mock_dag_run.conf = {}
        context = {"ti": mock_ti, "dag_run": mock_dag_run}

        with pytest.raises(Exception, match="Validation failed"):
            operator.execute(context)

        # Verify error was pushed to XCom
        mock_ti.xcom_push.assert_any_call(
            key="task_errors_validate",
            value=[{
                "task_id": "validate",
                "error_code": "VALIDATION_ERROR",
                "message": "Validation failed: Access denied",
            }],
        )


class TestExecuteS3ToMongoTask:
    """Test ExecuteS3ToMongoTask operator."""

    _mock_credentials = {
        "s3_endpoint_url": "http://minio:9000",
        "s3_access_key": "mykey", "s3_secret_key": "mysecret",
        "mongo_uri": "mongodb://user:pass@host:27017/",
        "mongo_database": "mydb",
    }

    def _make_execute_context(self):
        """Build a mock context for Execute task tests."""
        mock_ti = Mock()
        mock_ti.xcom_pull.side_effect = lambda task_ids=None, key=None: {
            ("prepare", None): {
                "s3_bucket": "test-bucket",
                "s3_prefix": "data/",
                "mongo_collection": "test_collection",
            },
            ("prepare", "traceparent"): None,
        }.get((task_ids, key))
        mock_dag_run = Mock()
        mock_dag_run.run_id = "test_run_123"
        mock_dag_run.conf = {}
        return {"ti": mock_ti, "dag_run": mock_dag_run}

    @patch("operators.s3_to_mongo_operators.fetch_credentials")
    @patch("operators.s3_to_mongo_operators.MongoClient")
    @patch("operators.s3_to_mongo_operators.MongoAuth")
    @patch("operators.s3_to_mongo_operators.S3Reader")
    @patch("operators.s3_to_mongo_operators.S3Client")
    @patch("operators.s3_to_mongo_operators.S3Auth")
    def test_execute_no_objects(
        self, mock_s3_auth_cls, mock_s3_client_cls, mock_s3_reader_cls,
        mock_mongo_auth_cls, mock_mongo_client_cls, mock_fetch_creds
    ):
        """Test execute with no S3 objects returns empty stats."""
        operator = ExecuteS3ToMongoTask(task_id="execute")

        mock_fetch_creds.return_value = self._mock_credentials
        mock_s3_client_cls.return_value.list_objects.return_value = []

        context = self._make_execute_context()

        result = operator.execute(context)

        assert result["files_processed"] == 0
        assert result["records_read"] == 0
        assert result["records_written"] == 0
        assert result["errors"] == 0
        mock_fetch_creds.assert_called_once_with("test_run_123")

    @patch("operators.s3_to_mongo_operators.fetch_credentials")
    @patch("operators.s3_to_mongo_operators.MongoClient")
    @patch("operators.s3_to_mongo_operators.MongoAuth")
    @patch("operators.s3_to_mongo_operators.S3Reader")
    @patch("operators.s3_to_mongo_operators.S3Client")
    @patch("operators.s3_to_mongo_operators.S3Auth")
    def test_execute_processes_json_files(
        self, mock_s3_auth_cls, mock_s3_client_cls, mock_s3_reader_cls,
        mock_mongo_auth_cls, mock_mongo_client_cls, mock_fetch_creds
    ):
        """Test execute processes JSON files and writes to MongoDB."""
        operator = ExecuteS3ToMongoTask(task_id="execute")
        mock_fetch_creds.return_value = self._mock_credentials

        mock_s3_client_cls.return_value.list_objects.return_value = [
            {"Key": "data/record_1.json"},
            {"Key": "data/record_2.json"},
        ]
        mock_s3_reader_cls.return_value.read_json.side_effect = [
            {"id": 1, "name": "Alice"},
            [{"id": 2, "name": "Bob"}, {"id": 3, "name": "Charlie"}],
        ]
        mock_mongo_client_cls.return_value.insert_many.return_value = ["id1", "id2", "id3"]

        context = self._make_execute_context()

        result = operator.execute(context)

        assert result["files_processed"] == 2
        assert result["records_read"] == 3
        assert result["errors"] == 0

        # No errors → no xcom_push for task_errors
        for c in context["ti"].xcom_push.call_args_list:
            assert "task_errors" not in str(c)

    @patch("operators.s3_to_mongo_operators.fetch_credentials")
    @patch("operators.s3_to_mongo_operators.MongoClient")
    @patch("operators.s3_to_mongo_operators.MongoAuth")
    @patch("operators.s3_to_mongo_operators.S3Reader")
    @patch("operators.s3_to_mongo_operators.S3Client")
    @patch("operators.s3_to_mongo_operators.S3Auth")
    def test_execute_pushes_data_errors_to_xcom(
        self, mock_s3_auth_cls, mock_s3_client_cls, mock_s3_reader_cls,
        mock_mongo_auth_cls, mock_mongo_client_cls, mock_fetch_creds
    ):
        """Test that data-level errors are pushed to XCom for CleanUp."""
        operator = ExecuteS3ToMongoTask(task_id="execute")
        mock_fetch_creds.return_value = self._mock_credentials

        mock_s3_client_cls.return_value.list_objects.return_value = [
            {"Key": "data/good.json"},
            {"Key": "data/bad.json"},
        ]
        mock_s3_reader_cls.return_value.read_json.side_effect = [
            {"id": 1, "name": "Alice"},
            Exception("corrupt file"),
        ]
        mock_mongo_client_cls.return_value.insert_many.return_value = ["id1"]

        context = self._make_execute_context()

        result = operator.execute(context)

        assert result["files_processed"] == 1
        assert result["errors"] == 1

        # Verify error was pushed to XCom
        context["ti"].xcom_push.assert_called_once()
        push_call = context["ti"].xcom_push.call_args
        assert push_call[1]["key"] == "task_errors_execute"
        errors = push_call[1]["value"]
        assert len(errors) == 1
        assert errors[0]["error_code"] == "DATA_ERROR"
        assert "corrupt file" in errors[0]["message"]


class TestCleanUpS3ToMongoTask:
    """Test CleanUpS3ToMongoTask operator."""

    def _make_context(
        self,
        config=None,
        stats=None,
        dag_run_conf=None,
        task_states=None,
        xcom_errors=None,
    ):
        """Helper to build a mock Airflow context.

        Args:
            xcom_errors: dict mapping task_id → list of error dicts.
                e.g. {"validate": [{"task_id": "validate", "error_code": "...", "message": "..."}]}
        """
        xcom_errors = xcom_errors or {}
        mock_ti = Mock()

        def xcom_pull_side_effect(task_ids=None, key=None):
            # XCom error keys (called by pull_all_task_errors with key= only)
            if key and key.startswith("task_errors_"):
                task_id = key.replace("task_errors_", "")
                return xcom_errors.get(task_id)
            # Standard XCom pulls (no credentials — those are in Redis now)
            if task_ids == "prepare" and key is None:
                return config
            if task_ids == "execute" and key is None:
                return stats
            return None

        mock_ti.xcom_pull.side_effect = xcom_pull_side_effect

        mock_dag_run = Mock()
        mock_dag_run.conf = dag_run_conf or {}
        mock_dag_run.dag_id = "test_dag"
        mock_dag_run.run_id = "test_run_123"

        # Mock task instances for upstream error checking (fallback)
        task_instances = []
        if task_states:
            for tid, state in task_states.items():
                ti_mock = Mock()
                ti_mock.task_id = tid
                ti_mock.state = state
                task_instances.append(ti_mock)
        mock_dag_run.get_task_instances.return_value = task_instances

        # Mock session for XCom clearing
        mock_session = Mock()
        mock_session.query.return_value.filter.return_value.delete.return_value = 1
        mock_ti.get_session.return_value = mock_session

        return {"ti": mock_ti, "dag_run": mock_dag_run}

    @patch("operators.s3_to_mongo_operators.delete_credentials")
    @patch("operators.s3_to_mongo_operators.create_control_plane_engine")
    def test_execute_success_updates_db(self, mock_create_engine, mock_delete_creds):
        """Test cleanup updates IntegrationRun on success (no errors in XCom)."""
        operator = CleanUpS3ToMongoTask(task_id="cleanup")

        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)

        # Simulate finding an IntegrationRun record
        mock_result = Mock()
        mock_result.fetchone.return_value = (42,)  # run_id = 42
        mock_conn.execute.return_value = mock_result

        context = self._make_context(
            config={"integration_id": 1, "s3_bucket": "b", "s3_prefix": "", "mongo_collection": "c"},
            task_states={"prepare": "success", "validate": "success", "execute": "success"},
        )

        with patch.dict(os.environ, {"CONTROL_PLANE_DB_URL": "mysql+pymysql://test"}):
            operator.execute(context)

        # Verify: SELECT (find run_id) + UPDATE (set ended/is_success) = 2 calls
        assert mock_conn.execute.call_count == 2
        # Verify commit was called
        mock_conn.commit.assert_called_once()

    @patch("operators.s3_to_mongo_operators.delete_credentials")
    @patch("operators.s3_to_mongo_operators.create_control_plane_engine")
    def test_execute_records_xcom_errors(self, mock_create_engine, mock_delete_creds):
        """Test cleanup persists detailed errors from XCom to integration_run_errors."""
        operator = CleanUpS3ToMongoTask(task_id="cleanup")

        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)

        mock_result = Mock()
        mock_result.fetchone.return_value = (42,)
        mock_conn.execute.return_value = mock_result

        context = self._make_context(
            config={"integration_id": 1},
            task_states={
                "prepare": "success",
                "validate": "failed",
                "execute": "upstream_failed",
            },
            xcom_errors={
                "validate": [
                    {"task_id": "validate", "error_code": "VALIDATION_ERROR", "message": "S3 bucket not found"},
                ],
            },
        )

        with patch.dict(os.environ, {"CONTROL_PLANE_DB_URL": "mysql+pymysql://test"}):
            operator.execute(context)

        # SELECT + UPDATE + 1 INSERT (validate XCom error) +
        # 1 INSERT (execute state fallback, since no XCom error for execute) = 4 calls
        assert mock_conn.execute.call_count == 4

    @patch("operators.s3_to_mongo_operators.delete_credentials")
    @patch("operators.s3_to_mongo_operators.create_control_plane_engine")
    def test_execute_records_data_errors_from_xcom(self, mock_create_engine, mock_delete_creds):
        """Test cleanup records data-level errors pushed by Execute task."""
        operator = CleanUpS3ToMongoTask(task_id="cleanup")

        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)

        mock_result = Mock()
        mock_result.fetchone.return_value = (42,)
        mock_conn.execute.return_value = mock_result

        context = self._make_context(
            config={"integration_id": 1},
            task_states={"prepare": "success", "validate": "success", "execute": "success"},
            xcom_errors={
                "execute": [
                    {"task_id": "execute", "error_code": "DATA_ERROR", "message": "JSON parse error in file1.json"},
                    {"task_id": "execute", "error_code": "DATA_ERROR", "message": "Error in file2.json"},
                ],
            },
        )

        with patch.dict(os.environ, {"CONTROL_PLANE_DB_URL": "mysql+pymysql://test"}):
            operator.execute(context)

        # SELECT + UPDATE + 2 INSERTs (two data errors from XCom) = 4 calls
        assert mock_conn.execute.call_count == 4

    @patch("operators.s3_to_mongo_operators.delete_credentials")
    @patch("operators.s3_to_mongo_operators.create_control_plane_engine")
    def test_execute_state_fallback_when_no_xcom_errors(self, mock_create_engine, mock_delete_creds):
        """Test that task state is used as fallback when XCom errors are absent."""
        operator = CleanUpS3ToMongoTask(task_id="cleanup")

        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)

        mock_result = Mock()
        mock_result.fetchone.return_value = (42,)
        mock_conn.execute.return_value = mock_result

        # No xcom_errors — task states used as fallback
        context = self._make_context(
            config={"integration_id": 1},
            task_states={
                "prepare": "success",
                "validate": "failed",
                "execute": "upstream_failed",
            },
        )

        with patch.dict(os.environ, {"CONTROL_PLANE_DB_URL": "mysql+pymysql://test"}):
            operator.execute(context)

        # SELECT + UPDATE + 2 INSERTs (validate failed + execute upstream_failed) = 4 calls
        assert mock_conn.execute.call_count == 4

    @patch("operators.s3_to_mongo_operators.delete_credentials")
    @patch("operators.s3_to_mongo_operators.create_control_plane_engine")
    def test_execute_no_duplicate_errors_from_xcom_and_state(self, mock_create_engine, mock_delete_creds):
        """Test that errors from XCom don't get duplicated by state fallback."""
        operator = CleanUpS3ToMongoTask(task_id="cleanup")

        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)

        mock_result = Mock()
        mock_result.fetchone.return_value = (42,)
        mock_conn.execute.return_value = mock_result

        # validate has both XCom error AND failed state
        context = self._make_context(
            config={"integration_id": 1},
            task_states={
                "prepare": "success",
                "validate": "failed",
                "execute": "upstream_failed",
            },
            xcom_errors={
                "validate": [
                    {"task_id": "validate", "error_code": "VALIDATION_ERROR", "message": "S3 bucket not found"},
                ],
            },
        )

        with patch.dict(os.environ, {"CONTROL_PLANE_DB_URL": "mysql+pymysql://test"}):
            operator.execute(context)

        # SELECT + UPDATE + 1 INSERT (validate from XCom, NOT duplicated by state) +
        # 1 INSERT (execute from state fallback) = 4 calls
        assert mock_conn.execute.call_count == 4

    @patch("operators.s3_to_mongo_operators.delete_credentials")
    def test_execute_no_integration_id_skips_db(self, mock_delete_creds):
        """Test cleanup handles missing integration_id gracefully."""
        operator = CleanUpS3ToMongoTask(task_id="cleanup")

        context = self._make_context(
            config=None,
            dag_run_conf={},
            task_states={"prepare": "failed"},
        )

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("CONTROL_PLANE_DB_URL", None)
            # Should not raise
            operator.execute(context)

    @patch("operators.s3_to_mongo_operators.delete_credentials")
    def test_execute_no_db_url_skips_db(self, mock_delete_creds):
        """Test cleanup skips DB update when CONTROL_PLANE_DB_URL not set."""
        operator = CleanUpS3ToMongoTask(task_id="cleanup")

        context = self._make_context(
            config={"integration_id": 1},
            task_states={"prepare": "success", "validate": "success", "execute": "success"},
        )

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("CONTROL_PLANE_DB_URL", None)
            # Should not raise — just logs warning
            operator.execute(context)

    @patch("operators.s3_to_mongo_operators.delete_credentials")
    @patch("operators.s3_to_mongo_operators.create_control_plane_engine")
    def test_execute_no_integration_run_record(self, mock_create_engine, mock_delete_creds):
        """Test cleanup handles missing IntegrationRun record gracefully."""
        operator = CleanUpS3ToMongoTask(task_id="cleanup")

        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)

        # No IntegrationRun record found
        mock_result = Mock()
        mock_result.fetchone.return_value = None
        mock_conn.execute.return_value = mock_result

        context = self._make_context(
            config={"integration_id": 1},
            task_states={"prepare": "success", "validate": "success", "execute": "success"},
        )

        with patch.dict(os.environ, {"CONTROL_PLANE_DB_URL": "mysql+pymysql://test"}):
            operator.execute(context)

        # Only SELECT was called (no UPDATE/INSERT since no record found)
        assert mock_conn.execute.call_count == 1

    @patch("operators.s3_to_mongo_operators.delete_credentials")
    def test_execute_fallback_to_dag_run_conf(self, mock_delete_creds):
        """Test cleanup falls back to dag_run.conf when XCom has no config."""
        operator = CleanUpS3ToMongoTask(task_id="cleanup")

        context = self._make_context(
            config=None,  # prepare failed, no XCom
            dag_run_conf={"integration_id": 99},
            task_states={"prepare": "failed", "validate": "upstream_failed", "execute": "upstream_failed"},
        )

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("CONTROL_PLANE_DB_URL", None)
            # Should not raise; integration_id=99 comes from dag_run.conf
            operator.execute(context)

    @patch("operators.s3_to_mongo_operators.delete_credentials")
    def test_execute_clears_credentials_from_redis(self, mock_delete_creds):
        """Test cleanup clears credentials from Redis vault."""
        operator = CleanUpS3ToMongoTask(task_id="cleanup")

        context = self._make_context(
            config={"integration_id": 1},
            task_states={"prepare": "success", "validate": "success", "execute": "success"},
        )

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("CONTROL_PLANE_DB_URL", None)
            operator.execute(context)

        # Verify Redis credentials were deleted
        mock_delete_creds.assert_called_once_with("test_run_123")

    @patch("operators.s3_to_mongo_operators.delete_credentials")
    @patch("operators.s3_to_mongo_operators.create_control_plane_engine")
    def test_execute_db_error_does_not_fail_cleanup(self, mock_create_engine, mock_delete_creds):
        """Test that DB errors don't cause cleanup task to fail."""
        operator = CleanUpS3ToMongoTask(task_id="cleanup")

        mock_create_engine.side_effect = Exception("DB connection failed")

        context = self._make_context(
            config={"integration_id": 1},
            task_states={"prepare": "success", "validate": "success", "execute": "success"},
        )

        with patch.dict(os.environ, {"CONTROL_PLANE_DB_URL": "mysql+pymysql://test"}):
            # Should not raise — DB errors are caught and logged
            operator.execute(context)

    @patch("operators.s3_to_mongo_operators.delete_credentials")
    def test_execute_with_none_stats(self, mock_delete_creds):
        """Test cleanup handles None stats (execute task failed before producing stats)."""
        operator = CleanUpS3ToMongoTask(task_id="cleanup")

        context = self._make_context(
            config={"integration_id": 1},
            stats=None,
            task_states={"prepare": "success", "validate": "success", "execute": "failed"},
        )

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("CONTROL_PLANE_DB_URL", None)
            # Should not raise
            operator.execute(context)


class TestDispatchScheduledIntegrationsTask:
    """Test DispatchScheduledIntegrationsTask operator."""

    def _make_integration_row(
        self,
        integration_id=1,
        workspace_id="ws-001",
        integration_type="s3_to_mongo",
        auth_id=10,
        source_access_pt_id=20,
        dest_access_pt_id=30,
        utc_sch_cron="0 2 * * *",
        json_data='{"s3_bucket": "my-bucket", "s3_prefix": "data/", "mongo_collection": "my_col"}',
    ):
        """Build a mock integration row matching the SQLAlchemy Row interface."""
        row = Mock()
        row.integration_id = integration_id
        row.workspace_id = workspace_id
        row.integration_type = integration_type
        row.auth_id = auth_id
        row.source_access_pt_id = source_access_pt_id
        row.dest_access_pt_id = dest_access_pt_id
        row.utc_sch_cron = utc_sch_cron
        row.json_data = json_data
        row.schedule_type = "daily"
        row.usr_sch_status = "active"
        return row

    def _make_auth_row(self, auth_type="aws_iam", json_data='{"s3_access_key": "ak", "s3_secret_key": "sk"}'):
        row = Mock()
        row.auth_type = auth_type
        row.json_data = json_data
        return row

    @patch("shared_utils.dag_trigger.get_airflow_auth_headers")
    @patch("shared_utils.dag_trigger.requests.post")
    @patch("operators.dispatch_operators.create_control_plane_engine")
    @patch("operators.dispatch_operators.get_control_plane_config")
    def test_dispatch_success(self, mock_config, mock_engine_cls, mock_post, mock_auth_headers):
        """Test successful dispatch of one integration (no integration_run creation)."""
        # Setup config
        config = Mock()
        config.control_plane_db_url = "mysql+pymysql://test"
        config.airflow_internal_api_url = "http://airflow:8080/api/v2"
        config.airflow_username = "airflow"
        config.airflow_password = "airflow"
        mock_config.return_value = config

        # Setup DB engine
        integration_row = self._make_integration_row()
        auth_row = self._make_auth_row()

        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine_cls.return_value = mock_engine

        # Track which query is being called
        call_count = {"n": 0}
        def execute_side_effect(query, *args, **kwargs):
            call_count["n"] += 1
            result = MagicMock()
            if call_count["n"] == 1:
                # First call: find integrations
                result.fetchall.return_value = [integration_row]
            elif call_count["n"] == 2:
                # Second call: find auth records
                result.fetchall.return_value = [auth_row]
            return result

        mock_conn.execute.side_effect = execute_side_effect
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)

        # Setup REST API response
        mock_auth_headers.return_value = {"Authorization": "Bearer token"}
        mock_post.return_value = Mock(status_code=200)
        mock_post.return_value.json.return_value = {"dag_run_id": "test_run_id"}
        mock_post.return_value.raise_for_status = Mock()

        # Execute
        operator = DispatchScheduledIntegrationsTask(
            task_id="dispatch", schedule_hour=2, integration_type="s3_to_mongo"
        )
        result = operator.execute({})

        # Verify
        assert result["dispatched"] == 1
        assert result["errors"] == 0
        assert len(result["results"]) == 1
        assert result["results"][0]["integration_id"] == 1
        assert result["results"][0]["status"] == "triggered"

        # Verify REST API was called
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        assert "s3_to_mongo_ondemand" in call_kwargs[0][0]
        payload = call_kwargs[1]["json"]
        assert payload["conf"]["s3_bucket"] == "my-bucket"
        assert payload["conf"]["integration_id"] == 1
        assert payload["conf"]["s3_access_key"] == "ak"

        # Verify: only 2 DB calls (find integrations + find auths)
        # No integration_run creation — that's now PrepareTask's job
        assert mock_conn.execute.call_count == 2

    @patch("operators.dispatch_operators.create_control_plane_engine")
    @patch("operators.dispatch_operators.get_control_plane_config")
    def test_dispatch_no_matching_integrations(self, mock_config, mock_engine_cls):
        """Test dispatch returns empty when no integrations match."""
        config = Mock()
        config.control_plane_db_url = "mysql+pymysql://test"
        mock_config.return_value = config

        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine_cls.return_value = mock_engine
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)

        operator = DispatchScheduledIntegrationsTask(
            task_id="dispatch", schedule_hour=2, integration_type="s3_to_mongo"
        )
        result = operator.execute({})

        assert result["dispatched"] == 0
        assert result["total"] == 0

    @patch("shared_utils.dag_trigger.get_airflow_auth_headers")
    @patch("shared_utils.dag_trigger.requests.post")
    @patch("operators.dispatch_operators.create_control_plane_engine")
    @patch("operators.dispatch_operators.get_control_plane_config")
    def test_dispatch_continues_on_error(self, mock_config, mock_engine_cls, mock_post, mock_auth_headers):
        """Test that one integration failing doesn't stop others."""
        config = Mock()
        config.control_plane_db_url = "mysql+pymysql://test"
        config.airflow_internal_api_url = "http://airflow:8080/api/v2"
        config.airflow_username = "airflow"
        config.airflow_password = "airflow"
        mock_config.return_value = config

        row1 = self._make_integration_row(integration_id=1)
        row2 = self._make_integration_row(integration_id=2, workspace_id="ws-002")
        auth_row = self._make_auth_row()

        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine_cls.return_value = mock_engine

        call_count = {"n": 0}
        def execute_side_effect(query, *args, **kwargs):
            call_count["n"] += 1
            result = MagicMock()
            if call_count["n"] == 1:
                result.fetchall.return_value = [row1, row2]
            else:
                result.fetchall.return_value = [auth_row]
            return result

        mock_conn.execute.side_effect = execute_side_effect
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)

        mock_auth_headers.return_value = {"Authorization": "Bearer token"}
        # First call fails, second succeeds
        mock_post.side_effect = [
            Exception("API error"),
            Mock(status_code=200, json=Mock(return_value={"dag_run_id": "run2"}), raise_for_status=Mock()),
        ]

        operator = DispatchScheduledIntegrationsTask(
            task_id="dispatch", schedule_hour=2, integration_type="s3_to_mongo"
        )
        result = operator.execute({})

        assert result["dispatched"] == 1
        assert result["errors"] == 1
        assert result["total"] == 2
        assert result["results"][0]["status"] == "error"
        assert result["results"][1]["status"] == "triggered"

    def test_extract_hour(self):
        """Test cron hour extraction."""
        assert DispatchScheduledIntegrationsTask._extract_hour("0 2 * * *") == 2
        assert DispatchScheduledIntegrationsTask._extract_hour("0 14 * * *") == 14
        assert DispatchScheduledIntegrationsTask._extract_hour("0 0 * * *") == 0
        assert DispatchScheduledIntegrationsTask._extract_hour("bad cron") is None
        assert DispatchScheduledIntegrationsTask._extract_hour("") is None

    @patch("operators.dispatch_operators.create_control_plane_engine")
    @patch("operators.dispatch_operators.get_control_plane_config")
    def test_dispatch_filters_by_hour(self, mock_config, mock_engine_cls):
        """Test that only integrations matching the schedule hour are dispatched."""
        config = Mock()
        config.control_plane_db_url = "mysql+pymysql://test"
        mock_config.return_value = config

        row_h2 = self._make_integration_row(integration_id=1, utc_sch_cron="0 2 * * *")
        row_h5 = self._make_integration_row(integration_id=2, utc_sch_cron="0 5 * * *")

        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine_cls.return_value = mock_engine
        mock_conn.execute.return_value.fetchall.return_value = [row_h2, row_h5]
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)

        operator = DispatchScheduledIntegrationsTask(
            task_id="dispatch", schedule_hour=2, integration_type="s3_to_mongo"
        )
        # Call _find_due_integrations directly to test filtering
        matched = operator._find_due_integrations(mock_engine)

        assert len(matched) == 1
        assert matched[0].integration_id == 1

    @patch("operators.dispatch_operators.get_control_plane_config")
    def test_dispatch_no_db_url(self, mock_config):
        """Test dispatch handles missing DB URL gracefully."""
        config = Mock()
        config.control_plane_db_url = ""
        mock_config.return_value = config

        operator = DispatchScheduledIntegrationsTask(
            task_id="dispatch", schedule_hour=2
        )
        result = operator.execute({})

        assert result["dispatched"] == 0

    @patch("operators.dispatch_operators.create_control_plane_engine")
    @patch("operators.dispatch_operators.get_control_plane_config")
    def test_weekly_dispatches_all_active(self, mock_config, mock_engine_cls):
        """Weekly dispatcher returns all active weekly integrations (no hour filter)."""
        config = Mock()
        config.control_plane_db_url = "mysql+pymysql://test"
        mock_config.return_value = config

        row1 = self._make_integration_row(integration_id=1, utc_sch_cron="0 3 * * 1")
        row2 = self._make_integration_row(integration_id=2, utc_sch_cron="0 5 * * 1")

        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine_cls.return_value = mock_engine
        mock_conn.execute.return_value.fetchall.return_value = [row1, row2]
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)

        operator = DispatchScheduledIntegrationsTask(
            task_id="dispatch", schedule_type="weekly", integration_type="s3_to_mongo"
        )
        matched = operator._find_due_integrations(mock_engine)

        # Weekly returns ALL active integrations regardless of cron hour
        assert len(matched) == 2

    @patch("operators.dispatch_operators.create_control_plane_engine")
    @patch("operators.dispatch_operators.get_control_plane_config")
    def test_monthly_dispatches_all_active(self, mock_config, mock_engine_cls):
        """Monthly dispatcher returns all active monthly integrations (no hour filter)."""
        config = Mock()
        config.control_plane_db_url = "mysql+pymysql://test"
        mock_config.return_value = config

        row1 = self._make_integration_row(integration_id=1, utc_sch_cron="0 4 1 * *")

        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine_cls.return_value = mock_engine
        mock_conn.execute.return_value.fetchall.return_value = [row1]
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)

        operator = DispatchScheduledIntegrationsTask(
            task_id="dispatch", schedule_type="monthly", integration_type="s3_to_mongo"
        )
        matched = operator._find_due_integrations(mock_engine)

        assert len(matched) == 1
        assert matched[0].integration_id == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
