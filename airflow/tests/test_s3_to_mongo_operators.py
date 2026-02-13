"""Unit tests for S3 to MongoDB operators.

These tests mock the Airflow framework dependencies so they can run
without Apache Airflow installed (e.g., in the host dev environment).
"""

import pytest
import os
import sys
from unittest.mock import Mock, MagicMock, patch, PropertyMock
from types import ModuleType
from datetime import datetime


# ---------------------------------------------------------------------------
# Mock Airflow modules so operators can be imported without Airflow installed
# ---------------------------------------------------------------------------
def _setup_airflow_mocks():
    """Create mock Airflow modules and inject them into sys.modules."""
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

    # apply_defaults is a no-op decorator in modern Airflow
    def apply_defaults(func):
        return func

    # Build mock module tree
    airflow_mod = ModuleType("airflow")
    airflow_models = ModuleType("airflow.models")
    airflow_utils = ModuleType("airflow.utils")
    airflow_utils_decorators = ModuleType("airflow.utils.decorators")
    airflow_utils_trigger_rule = ModuleType("airflow.utils.trigger_rule")

    airflow_models.BaseOperator = MockBaseOperator
    airflow_models.XCom = MockXCom
    airflow_utils_decorators.apply_defaults = apply_defaults

    # TriggerRule enum mock
    class TriggerRule:
        ALL_SUCCESS = "all_success"
        ALL_DONE = "all_done"

    airflow_utils_trigger_rule.TriggerRule = TriggerRule

    sys.modules["airflow"] = airflow_mod
    sys.modules["airflow.models"] = airflow_models
    sys.modules["airflow.utils"] = airflow_utils
    sys.modules["airflow.utils.decorators"] = airflow_utils_decorators
    sys.modules["airflow.utils.trigger_rule"] = airflow_utils_trigger_rule

    return MockXCom


# Check if real Airflow is available; if not, use mocks
try:
    from airflow.models import BaseOperator  # noqa: F401
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


class TestPrepareS3ToMongoTask:
    """Test PrepareS3ToMongoTask operator."""

    def test_execute_success(self):
        """Test successful execution pushes config and credentials."""
        operator = PrepareS3ToMongoTask(task_id="prepare")

        mock_ti = Mock()
        mock_dag_run = Mock()
        mock_dag_run.conf = {
            "integration_id": 1,
            "s3_bucket": "test-bucket",
            "s3_prefix": "data/",
            "mongo_collection": "test_collection",
            "s3_endpoint_url": "http://custom-s3:9000",
            "s3_access_key": "mykey",
            "s3_secret_key": "mysecret",
            "mongo_uri": "mongodb://user:pass@host:27017/",
            "mongo_database": "mydb",
        }
        context = {"dag_run": mock_dag_run, "ti": mock_ti}

        result = operator.execute(context)

        # Config return value should NOT contain credentials
        assert result["s3_bucket"] == "test-bucket"
        assert result["s3_prefix"] == "data/"
        assert result["mongo_collection"] == "test_collection"
        assert result["integration_id"] == 1
        assert "s3_access_key" not in result
        assert "s3_secret_key" not in result
        assert "mongo_uri" not in result

        # Credentials should be pushed as separate XCom key
        mock_ti.xcom_push.assert_called_once_with(
            key="credentials",
            value={
                "s3_endpoint_url": "http://custom-s3:9000",
                "s3_access_key": "mykey",
                "s3_secret_key": "mysecret",
                "mongo_uri": "mongodb://user:pass@host:27017/",
                "mongo_database": "mydb",
            },
        )

    def test_execute_credentials_use_defaults(self):
        """Test that missing credentials use defaults."""
        operator = PrepareS3ToMongoTask(task_id="prepare")

        mock_ti = Mock()
        mock_dag_run = Mock()
        mock_dag_run.conf = {
            "s3_bucket": "test-bucket",
            "mongo_collection": "test_collection",
        }
        context = {"dag_run": mock_dag_run, "ti": mock_ti}

        operator.execute(context)

        # Verify defaults are used in credentials
        creds = mock_ti.xcom_push.call_args[1]["value"]
        assert creds["s3_endpoint_url"] == "http://minio:9000"
        assert creds["s3_access_key"] == "minioadmin"
        assert creds["s3_secret_key"] == "minioadmin"
        assert creds["mongo_uri"] == "mongodb://root:root@mongodb:27017/"
        assert creds["mongo_database"] == "test_database"

    def test_execute_missing_s3_bucket(self):
        """Test execution fails when s3_bucket is missing."""
        operator = PrepareS3ToMongoTask(task_id="prepare")

        mock_ti = Mock()
        mock_dag_run = Mock()
        mock_dag_run.conf = {"mongo_collection": "test_collection"}
        context = {"dag_run": mock_dag_run, "ti": mock_ti}

        with pytest.raises(ValueError, match="s3_bucket is required"):
            operator.execute(context)

    def test_execute_missing_mongo_collection(self):
        """Test execution fails when mongo_collection is missing."""
        operator = PrepareS3ToMongoTask(task_id="prepare")

        mock_ti = Mock()
        mock_dag_run = Mock()
        mock_dag_run.conf = {"s3_bucket": "test-bucket"}
        context = {"dag_run": mock_dag_run, "ti": mock_ti}

        with pytest.raises(ValueError, match="mongo_collection is required"):
            operator.execute(context)

    def test_execute_with_default_prefix(self):
        """Test execution with default s3_prefix."""
        operator = PrepareS3ToMongoTask(task_id="prepare")

        mock_ti = Mock()
        mock_dag_run = Mock()
        mock_dag_run.conf = {
            "s3_bucket": "test-bucket",
            "mongo_collection": "test_collection",
        }
        context = {"dag_run": mock_dag_run, "ti": mock_ti}

        result = operator.execute(context)
        assert result["s3_prefix"] == ""

    def test_execute_with_empty_conf(self):
        """Test execution with None conf."""
        operator = PrepareS3ToMongoTask(task_id="prepare")

        mock_ti = Mock()
        mock_dag_run = Mock()
        mock_dag_run.conf = None
        context = {"dag_run": mock_dag_run, "ti": mock_ti}

        with pytest.raises(ValueError):
            operator.execute(context)


class TestValidateS3ToMongoTask:
    """Test ValidateS3ToMongoTask operator."""

    @patch("operators.s3_to_mongo_operators.MongoClient")
    @patch("operators.s3_to_mongo_operators.MongoAuth")
    @patch("operators.s3_to_mongo_operators.S3Client")
    @patch("operators.s3_to_mongo_operators.S3Auth")
    def test_execute_pulls_credentials_from_xcom(
        self, mock_s3_auth_cls, mock_s3_client_cls, mock_mongo_auth_cls, mock_mongo_client_cls
    ):
        """Test that validate reads credentials from XCom, not dag_run.conf."""
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

        mock_ti = Mock()

        def xcom_pull_side_effect(task_ids=None, key=None):
            if task_ids == "prepare" and key == "credentials":
                return credentials
            if task_ids == "prepare":
                return config
            return None

        mock_ti.xcom_pull.side_effect = xcom_pull_side_effect
        context = {"ti": mock_ti}

        result = operator.execute(context)
        assert result is True

        # Verify credentials were pulled from XCom
        mock_ti.xcom_pull.assert_any_call(task_ids="prepare", key="credentials")

        # Verify S3Auth was created with XCom credentials
        mock_s3_auth_cls.assert_called_once_with(
            aws_access_key_id="mykey",
            aws_secret_access_key="mysecret",
            region_name="us-east-1",
            endpoint_url="http://minio:9000",
        )

    @patch("operators.s3_to_mongo_operators.MongoClient")
    @patch("operators.s3_to_mongo_operators.MongoAuth")
    @patch("operators.s3_to_mongo_operators.S3Client")
    @patch("operators.s3_to_mongo_operators.S3Auth")
    def test_execute_validation_failure(
        self, mock_s3_auth_cls, mock_s3_client_cls, mock_mongo_auth_cls, mock_mongo_client_cls
    ):
        """Test that validation failure raises exception."""
        operator = ValidateS3ToMongoTask(task_id="validate")

        mock_s3_client_cls.return_value.list_objects.side_effect = Exception("Access denied")

        mock_ti = Mock()
        mock_ti.xcom_pull.side_effect = lambda task_ids=None, key=None: {
            ("prepare", None): {"s3_bucket": "bad-bucket", "s3_prefix": ""},
            ("prepare", "credentials"): {
                "s3_endpoint_url": "http://minio:9000",
                "s3_access_key": "bad", "s3_secret_key": "bad",
                "mongo_uri": "mongodb://host:27017/", "mongo_database": "db",
            },
        }[(task_ids, key)]
        context = {"ti": mock_ti}

        with pytest.raises(Exception, match="Validation failed"):
            operator.execute(context)


class TestExecuteS3ToMongoTask:
    """Test ExecuteS3ToMongoTask operator."""

    @patch("operators.s3_to_mongo_operators.MongoClient")
    @patch("operators.s3_to_mongo_operators.MongoAuth")
    @patch("operators.s3_to_mongo_operators.S3Reader")
    @patch("operators.s3_to_mongo_operators.S3Client")
    @patch("operators.s3_to_mongo_operators.S3Auth")
    def test_execute_no_objects(
        self, mock_s3_auth_cls, mock_s3_client_cls, mock_s3_reader_cls,
        mock_mongo_auth_cls, mock_mongo_client_cls
    ):
        """Test execute with no S3 objects returns empty stats."""
        operator = ExecuteS3ToMongoTask(task_id="execute")

        mock_s3_client_cls.return_value.list_objects.return_value = []

        mock_ti = Mock()
        mock_ti.xcom_pull.side_effect = lambda task_ids=None, key=None: {
            ("prepare", None): {
                "s3_bucket": "test-bucket",
                "s3_prefix": "data/",
                "mongo_collection": "test_collection",
            },
            ("prepare", "credentials"): {
                "s3_endpoint_url": "http://minio:9000",
                "s3_access_key": "mykey", "s3_secret_key": "mysecret",
                "mongo_uri": "mongodb://user:pass@host:27017/",
                "mongo_database": "mydb",
            },
        }[(task_ids, key)]
        context = {"ti": mock_ti}

        result = operator.execute(context)

        assert result["files_processed"] == 0
        assert result["records_read"] == 0
        assert result["records_written"] == 0
        assert result["errors"] == 0
        mock_ti.xcom_pull.assert_any_call(task_ids="prepare", key="credentials")

    @patch("operators.s3_to_mongo_operators.MongoClient")
    @patch("operators.s3_to_mongo_operators.MongoAuth")
    @patch("operators.s3_to_mongo_operators.S3Reader")
    @patch("operators.s3_to_mongo_operators.S3Client")
    @patch("operators.s3_to_mongo_operators.S3Auth")
    def test_execute_processes_json_files(
        self, mock_s3_auth_cls, mock_s3_client_cls, mock_s3_reader_cls,
        mock_mongo_auth_cls, mock_mongo_client_cls
    ):
        """Test execute processes JSON files and writes to MongoDB."""
        operator = ExecuteS3ToMongoTask(task_id="execute")

        mock_s3_client_cls.return_value.list_objects.return_value = [
            {"Key": "data/record_1.json"},
            {"Key": "data/record_2.json"},
        ]
        mock_s3_reader_cls.return_value.read_json.side_effect = [
            {"id": 1, "name": "Alice"},
            [{"id": 2, "name": "Bob"}, {"id": 3, "name": "Charlie"}],
        ]
        mock_mongo_client_cls.return_value.insert_many.return_value = ["id1", "id2", "id3"]

        mock_ti = Mock()
        mock_ti.xcom_pull.side_effect = lambda task_ids=None, key=None: {
            ("prepare", None): {
                "s3_bucket": "test-bucket",
                "s3_prefix": "data/",
                "mongo_collection": "test_collection",
            },
            ("prepare", "credentials"): {
                "s3_endpoint_url": "http://minio:9000",
                "s3_access_key": "mykey", "s3_secret_key": "mysecret",
                "mongo_uri": "mongodb://user:pass@host:27017/",
                "mongo_database": "mydb",
            },
        }[(task_ids, key)]
        context = {"ti": mock_ti}

        result = operator.execute(context)

        assert result["files_processed"] == 2
        assert result["records_read"] == 3
        assert result["errors"] == 0


class TestCleanUpS3ToMongoTask:
    """Test CleanUpS3ToMongoTask operator."""

    def _make_context(
        self,
        config=None,
        stats=None,
        dag_run_conf=None,
        task_states=None,
    ):
        """Helper to build a mock Airflow context."""
        mock_ti = Mock()

        def xcom_pull_side_effect(task_ids=None, key=None):
            if task_ids == "prepare" and key == "credentials":
                return {"s3_access_key": "secret"}
            if task_ids == "prepare" and key is None:
                return config
            if task_ids == "execute":
                return stats
            return None

        mock_ti.xcom_pull.side_effect = xcom_pull_side_effect

        mock_dag_run = Mock()
        mock_dag_run.conf = dag_run_conf or {}
        mock_dag_run.dag_id = "test_dag"
        mock_dag_run.run_id = "test_run_123"

        # Mock task instances for upstream error checking
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

    @patch("operators.s3_to_mongo_operators.create_engine")
    def test_execute_success_updates_db(self, mock_create_engine):
        """Test cleanup updates IntegrationRun on success."""
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
            stats={"files_processed": 5, "records_read": 100, "records_written": 100, "errors": 0, "error_messages": []},
            task_states={"prepare": "success", "validate": "success", "execute": "success"},
        )

        with patch.dict(os.environ, {"CONTROL_PLANE_DB_URL": "mysql+pymysql://test"}):
            operator.execute(context)

        # Verify: SELECT (find run_id) + UPDATE (set ended/is_success) = 2 calls
        assert mock_conn.execute.call_count == 2
        # Verify commit was called
        mock_conn.commit.assert_called_once()

    @patch("operators.s3_to_mongo_operators.create_engine")
    def test_execute_records_upstream_failures(self, mock_create_engine):
        """Test cleanup records errors when upstream tasks fail."""
        operator = CleanUpS3ToMongoTask(task_id="cleanup")

        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)

        # Simulate finding an IntegrationRun record
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
        )

        with patch.dict(os.environ, {"CONTROL_PLANE_DB_URL": "mysql+pymysql://test"}):
            operator.execute(context)

        # SELECT + UPDATE + 2 INSERTs (validate failed + execute upstream_failed) = 4 calls
        assert mock_conn.execute.call_count == 4

    @patch("operators.s3_to_mongo_operators.create_engine")
    def test_execute_records_data_errors_from_stats(self, mock_create_engine):
        """Test cleanup records data-level errors from execute stats."""
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
            stats={
                "files_processed": 3, "records_read": 10, "records_written": 8,
                "errors": 2, "error_messages": ["JSON parse error in file1.json", "Error in file2.json"],
            },
            task_states={"prepare": "success", "validate": "success", "execute": "success"},
        )

        with patch.dict(os.environ, {"CONTROL_PLANE_DB_URL": "mysql+pymysql://test"}):
            operator.execute(context)

        # SELECT + UPDATE + 2 INSERTs (two data errors) = 4 calls
        assert mock_conn.execute.call_count == 4

    def test_execute_no_integration_id_skips_db(self):
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

    def test_execute_no_db_url_skips_db(self):
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

    @patch("operators.s3_to_mongo_operators.create_engine")
    def test_execute_no_integration_run_record(self, mock_create_engine):
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

    def test_execute_fallback_to_dag_run_conf(self):
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

    def test_execute_clears_credentials_xcom(self):
        """Test cleanup clears sensitive XCom data."""
        operator = CleanUpS3ToMongoTask(task_id="cleanup")

        context = self._make_context(
            config={"integration_id": 1},
            task_states={"prepare": "success", "validate": "success", "execute": "success"},
        )

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("CONTROL_PLANE_DB_URL", None)
            operator.execute(context)

        # Verify XCom delete was called via session.query
        mock_session = context["ti"].get_session.return_value
        mock_session.query.assert_called()
        mock_session.commit.assert_called()

    @patch("operators.s3_to_mongo_operators.create_engine")
    def test_execute_db_error_does_not_fail_cleanup(self, mock_create_engine):
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

    def test_execute_with_none_stats(self):
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
