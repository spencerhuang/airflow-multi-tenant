"""Unit tests for shared_utils.dag_trigger module."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import pytest

from shared_utils.dag_trigger import (
    build_integration_conf,
    determine_dag_id,
    merge_json_data,
    resolve_auth_credentials_sync,
    trigger_airflow_dag,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_integration_row(**overrides):
    """Create a mock integration row with sensible defaults."""
    defaults = {
        "workspace_id": "ws-abc",
        "integration_id": 42,
        "integration_type": "s3_to_mongo",
        "auth_id": 7,
        "source_access_pt_id": 10,
        "dest_access_pt_id": 20,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


# ---------------------------------------------------------------------------
# build_integration_conf
# ---------------------------------------------------------------------------

class TestBuildIntegrationConf:
    def test_builds_correct_keys(self):
        row = _make_integration_row()
        conf = build_integration_conf(row)

        assert conf == {
            "tenant_id": "ws-abc",
            "integration_id": 42,
            "integration_type": "s3_to_mongo",
            "auth_id": 7,
            "source_access_pt_id": 10,
            "dest_access_pt_id": 20,
        }

    def test_maps_workspace_id_to_tenant_id(self):
        row = _make_integration_row(workspace_id="custom-ws")
        conf = build_integration_conf(row)
        assert conf["tenant_id"] == "custom-ws"

    def test_works_with_none_values(self):
        row = _make_integration_row(auth_id=None, source_access_pt_id=None)
        conf = build_integration_conf(row)
        assert conf["auth_id"] is None
        assert conf["source_access_pt_id"] is None


# ---------------------------------------------------------------------------
# merge_json_data
# ---------------------------------------------------------------------------

class TestMergeJsonData:
    def test_merges_valid_json(self):
        conf = {"existing": "value"}
        result = merge_json_data(conf, '{"new_key": "new_value"}')
        assert result is conf
        assert conf["new_key"] == "new_value"
        assert conf["existing"] == "value"

    def test_handles_none(self):
        conf = {"key": "val"}
        merge_json_data(conf, None)
        assert conf == {"key": "val"}

    def test_handles_empty_string(self):
        conf = {"key": "val"}
        merge_json_data(conf, "")
        assert conf == {"key": "val"}

    def test_logs_warning_on_invalid_json(self):
        mock_log = MagicMock()
        conf = {"key": "val"}
        merge_json_data(conf, "not-valid-json", log=mock_log)
        assert conf == {"key": "val"}
        mock_log.warning.assert_called_once()

    def test_silent_on_invalid_json_without_logger(self):
        conf = {"key": "val"}
        # Should not raise
        merge_json_data(conf, "{bad json")
        assert conf == {"key": "val"}

    def test_overwrites_existing_keys(self):
        conf = {"key": "old"}
        merge_json_data(conf, '{"key": "new"}')
        assert conf["key"] == "new"


# ---------------------------------------------------------------------------
# resolve_auth_credentials_sync
# ---------------------------------------------------------------------------

class TestResolveAuthCredentialsSync:
    def test_merges_auth_records(self):
        # Build mock engine + connection
        mock_row1 = SimpleNamespace(
            auth_type="aws_iam",
            json_data='{"s3_access_key": "AKIA123"}',
        )
        mock_row2 = SimpleNamespace(
            auth_type="mongo",
            json_data='{"mongo_uri": "mongodb://host:27017"}',
        )
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [mock_row1, mock_row2]

        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=False)

        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn

        creds = resolve_auth_credentials_sync(mock_engine, "ws-abc")

        assert creds["s3_access_key"] == "AKIA123"
        assert creds["mongo_uri"] == "mongodb://host:27017"

    def test_handles_invalid_json_in_auth(self):
        mock_row = SimpleNamespace(auth_type="bad", json_data="not-json")
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [mock_row]

        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=False)

        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn

        mock_log = MagicMock()
        creds = resolve_auth_credentials_sync(mock_engine, "ws-abc", log=mock_log)

        assert creds == {}
        mock_log.warning.assert_called_once()

    def test_handles_none_json_data(self):
        mock_row = SimpleNamespace(auth_type="empty", json_data=None)
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [mock_row]

        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=False)

        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn

        creds = resolve_auth_credentials_sync(mock_engine, "ws-abc")
        assert creds == {}

    def test_returns_empty_dict_when_no_auth_records(self):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []

        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=False)

        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn

        creds = resolve_auth_credentials_sync(mock_engine, "ws-abc")
        assert creds == {}


# ---------------------------------------------------------------------------
# determine_dag_id
# ---------------------------------------------------------------------------

class TestDetermineDagId:
    """Tests use CamelCase integration_type (e.g. 'S3ToMongo') which is
    the format stored in the database.  The function lowercases and
    replaces 'to' with '_to_' to produce DAG names like 's3_to_mongo'."""

    def test_daily_with_valid_cron(self):
        assert determine_dag_id("S3ToMongo", "daily", "0 2 * * *") == "s3_to_mongo_daily_02"

    def test_daily_with_two_digit_hour(self):
        assert determine_dag_id("S3ToMongo", "daily", "0 14 * * *") == "s3_to_mongo_daily_14"

    def test_daily_without_cron(self):
        assert determine_dag_id("S3ToMongo", "daily", None) == "s3_to_mongo_ondemand"

    def test_weekly(self):
        assert determine_dag_id("S3ToMongo", "weekly", "0 2 * * 1") == "s3_to_mongo_ondemand"

    def test_monthly(self):
        assert determine_dag_id("S3ToMongo", "monthly", "0 2 15 * *") == "s3_to_mongo_ondemand"

    def test_on_demand(self):
        assert determine_dag_id("S3ToMongo", "on_demand") == "s3_to_mongo_ondemand"

    def test_invalid_cron(self):
        assert determine_dag_id("S3ToMongo", "daily", "invalid") == "s3_to_mongo_ondemand"

    def test_daily_zero_hour(self):
        assert determine_dag_id("S3ToMongo", "daily", "0 0 * * *") == "s3_to_mongo_daily_00"

    def test_already_snake_case_input(self):
        # When integration_type is already snake_case (e.g. from dispatcher),
        # the "to" replacement doubles underscores. Real data uses CamelCase.
        result = determine_dag_id("s3_to_mongo", "on_demand")
        assert result == "s3__to__mongo_ondemand"


# ---------------------------------------------------------------------------
# trigger_airflow_dag
# ---------------------------------------------------------------------------

class TestTriggerAirflowDag:
    @patch("shared_utils.dag_trigger.get_airflow_auth_headers")
    @patch("shared_utils.dag_trigger.requests.post")
    def test_success(self, mock_post, mock_auth):
        mock_auth.return_value = {"Authorization": "Bearer tok"}
        mock_resp = Mock()
        mock_resp.json.return_value = {"dag_run_id": "run-123"}
        mock_post.return_value = mock_resp

        result = trigger_airflow_dag(
            "http://airflow:8080/api/v2", "user", "pass",
            "s3_to_mongo_ondemand", {"tenant_id": "ws-1"},
        )

        assert result == "run-123"
        mock_post.assert_called_once()
        # URL is passed as first positional arg
        call_args = mock_post.call_args
        url = call_args[0][0] if call_args[0] else call_args[1].get("url", "")
        assert "s3_to_mongo_ondemand" in url

    @patch("shared_utils.dag_trigger.get_airflow_auth_headers")
    @patch("shared_utils.dag_trigger.requests.post")
    def test_custom_trigger_source(self, mock_post, mock_auth):
        mock_auth.return_value = {"Authorization": "Bearer tok"}
        mock_resp = Mock()
        mock_resp.json.return_value = {"dag_run_id": "sched-456"}
        mock_post.return_value = mock_resp

        result = trigger_airflow_dag(
            "http://airflow:8080/api/v2", "user", "pass",
            "s3_to_mongo_ondemand", {"tenant_id": "ws-1"},
            trigger_source="scheduled",
        )

        assert result == "sched-456"
        payload = mock_post.call_args[1]["json"]
        assert "_scheduled_" in payload["dag_run_id"]

    @patch("shared_utils.dag_trigger.get_airflow_auth_headers")
    @patch("shared_utils.dag_trigger.requests.post")
    def test_fallback_run_id_on_missing_response_key(self, mock_post, mock_auth):
        mock_auth.return_value = {"Authorization": "Bearer tok"}
        mock_resp = Mock()
        mock_resp.json.return_value = {}  # No dag_run_id in response
        mock_post.return_value = mock_resp

        result = trigger_airflow_dag(
            "http://airflow:8080/api/v2", "user", "pass",
            "s3_to_mongo_ondemand", {"tenant_id": "ws-1"},
        )

        # Falls back to custom_run_id
        assert "ws-1_s3_to_mongo_ondemand_manual_" in result

    @patch("shared_utils.dag_trigger.get_airflow_auth_headers")
    @patch("shared_utils.dag_trigger.requests.post")
    def test_raises_on_http_error(self, mock_post, mock_auth):
        import requests
        mock_auth.return_value = {"Authorization": "Bearer tok"}
        mock_post.side_effect = requests.exceptions.ConnectionError("refused")

        with pytest.raises(requests.exceptions.ConnectionError):
            trigger_airflow_dag(
                "http://airflow:8080/api/v2", "user", "pass",
                "s3_to_mongo_ondemand", {"tenant_id": "ws-1"},
            )

    @patch("shared_utils.dag_trigger.get_airflow_auth_headers")
    @patch("shared_utils.dag_trigger.requests.post")
    def test_payload_structure(self, mock_post, mock_auth):
        mock_auth.return_value = {"Authorization": "Bearer tok"}
        mock_resp = Mock()
        mock_resp.json.return_value = {"dag_run_id": "run-x"}
        mock_post.return_value = mock_resp

        conf = {"tenant_id": "ws-1", "integration_id": 42}
        trigger_airflow_dag(
            "http://airflow:8080/api/v2", "user", "pass",
            "my_dag", conf,
        )

        payload = mock_post.call_args[1]["json"]
        assert "dag_run_id" in payload
        assert "logical_date" in payload
        assert payload["conf"] is conf
