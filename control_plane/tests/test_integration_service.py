"""Unit tests for Integration Service."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import json

from control_plane.app.services.integration_service import IntegrationService
from control_plane.app.models.integration import Integration
from control_plane.app.models.integration_run import IntegrationRun
from control_plane.app.schemas.integration import IntegrationCreate, IntegrationUpdate


class TestIntegrationService:
    """Test IntegrationService class."""

    @pytest.fixture
    def mock_db(self):
        """Fixture for mocked database session."""
        return Mock()

    @pytest.fixture
    def service(self, mock_db):
        """Fixture for IntegrationService instance."""
        return IntegrationService(mock_db)

    @pytest.fixture
    def sample_integration_create(self):
        """Fixture for sample integration creation data."""
        return IntegrationCreate(
            workspace_id="ws-123",
            workflow_id=1,
            auth_id=1,
            source_access_pt_id=1,
            dest_access_pt_id=2,
            integration_type="S3ToMongo",
            usr_sch_cron="0 2 * * *",
            usr_timezone="UTC",  # Changed to UTC for simpler testing
            schedule_type="daily",
            json_data='{"s3_bucket": "test-bucket"}',
        )

    @pytest.fixture
    def sample_integration(self):
        """Fixture for sample integration instance."""
        return Integration(
            integration_id=1,
            workspace_id="ws-123",
            workflow_id=1,
            auth_id=1,
            source_access_pt_id=1,
            dest_access_pt_id=2,
            integration_type="S3ToMongo",
            usr_sch_cron="0 2 * * *",
            usr_timezone="UTC",  # Changed to UTC for simpler testing
            utc_sch_cron="0 2 * * *",  # Added: UTC cron (same as usr for UTC timezone)
            schedule_type="daily",
            usr_sch_status="active",
            json_data='{"s3_bucket": "test-bucket"}',
        )

    def test_create_integration(self, service, mock_db, sample_integration_create):
        """Test creating a new integration."""
        service.create_integration(sample_integration_create)

        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()
        mock_db.refresh.assert_called_once()

        added_integration = mock_db.add.call_args[0][0]
        assert added_integration.workspace_id == "ws-123"
        assert added_integration.integration_type == "S3ToMongo"
        assert added_integration.usr_sch_status == "active"

    def test_get_integration_found(self, service, mock_db, sample_integration):
        """Test getting an integration that exists."""
        mock_query = Mock()
        mock_filter = Mock()
        mock_filter.first.return_value = sample_integration

        mock_query.filter.return_value = mock_filter
        mock_db.query.return_value = mock_query

        result = service.get_integration(1)

        assert result == sample_integration
        mock_db.query.assert_called_once_with(Integration)

    def test_get_integration_not_found(self, service, mock_db):
        """Test getting an integration that doesn't exist."""
        mock_query = Mock()
        mock_filter = Mock()
        mock_filter.first.return_value = None

        mock_query.filter.return_value = mock_filter
        mock_db.query.return_value = mock_query

        result = service.get_integration(999)

        assert result is None

    def test_list_integrations_no_filter(self, service, mock_db, sample_integration):
        """Test listing all integrations without workspace filter."""
        mock_query = Mock()
        mock_offset = Mock()
        mock_limit = Mock()
        mock_limit.all.return_value = [sample_integration]

        mock_offset.limit.return_value = mock_limit
        mock_query.offset.return_value = mock_offset
        mock_db.query.return_value = mock_query

        results = service.list_integrations()

        assert len(results) == 1
        assert results[0] == sample_integration
        mock_query.offset.assert_called_once_with(0)
        mock_offset.limit.assert_called_once_with(100)

    def test_list_integrations_with_workspace_filter(self, service, mock_db):
        """Test listing integrations with workspace filter."""
        mock_query = Mock()
        mock_filter = Mock()
        mock_offset = Mock()
        mock_limit = Mock()
        mock_limit.all.return_value = []

        mock_offset.limit.return_value = mock_limit
        mock_filter.offset.return_value = mock_offset
        mock_query.filter.return_value = mock_filter
        mock_db.query.return_value = mock_query

        service.list_integrations(workspace_id="ws-123", skip=10, limit=50)

        mock_query.filter.assert_called_once()
        mock_filter.offset.assert_called_once_with(10)
        mock_offset.limit.assert_called_once_with(50)

    def test_update_integration_success(self, service, mock_db, sample_integration):
        """Test updating an integration successfully."""
        update_data = IntegrationUpdate(
            usr_sch_status="paused",
            usr_sch_cron="0 3 * * *",
        )

        # Mock get_integration
        mock_query = Mock()
        mock_filter = Mock()
        mock_filter.first.return_value = sample_integration

        mock_query.filter.return_value = mock_filter
        mock_db.query.return_value = mock_query

        result = service.update_integration(1, update_data)

        assert result == sample_integration
        assert sample_integration.usr_sch_status == "paused"
        assert sample_integration.usr_sch_cron == "0 3 * * *"
        mock_db.commit.assert_called_once()
        mock_db.refresh.assert_called_once()

    def test_update_integration_not_found(self, service, mock_db):
        """Test updating a non-existent integration."""
        update_data = IntegrationUpdate(usr_sch_status="paused")

        # Mock get_integration returning None
        mock_query = Mock()
        mock_filter = Mock()
        mock_filter.first.return_value = None

        mock_query.filter.return_value = mock_filter
        mock_db.query.return_value = mock_query

        result = service.update_integration(999, update_data)

        assert result is None
        mock_db.commit.assert_not_called()

    def test_delete_integration_success(self, service, mock_db, sample_integration):
        """Test deleting an integration successfully."""
        # Mock get_integration
        mock_query = Mock()
        mock_filter = Mock()
        mock_filter.first.return_value = sample_integration

        mock_query.filter.return_value = mock_filter
        mock_db.query.return_value = mock_query

        result = service.delete_integration(1)

        assert result is True
        mock_db.delete.assert_called_once_with(sample_integration)
        mock_db.commit.assert_called_once()

    def test_delete_integration_not_found(self, service, mock_db):
        """Test deleting a non-existent integration."""
        # Mock get_integration returning None
        mock_query = Mock()
        mock_filter = Mock()
        mock_filter.first.return_value = None

        mock_query.filter.return_value = mock_filter
        mock_db.query.return_value = mock_query

        result = service.delete_integration(999)

        assert result is False
        mock_db.delete.assert_not_called()

    def test_get_dag_id_for_integration_daily(self, service, sample_integration):
        """Test getting DAG ID for daily integration."""
        dag_id = service._get_dag_id_for_integration(sample_integration)
        assert dag_id == "s3_to_mongo_daily_02"

    def test_get_dag_id_for_integration_ondemand(self, service, sample_integration):
        """Test getting DAG ID for on-demand integration."""
        sample_integration.schedule_type = "on_demand"
        sample_integration.usr_sch_cron = None
        sample_integration.utc_sch_cron = None

        dag_id = service._get_dag_id_for_integration(sample_integration)
        assert dag_id == "s3_to_mongo_ondemand"

    def test_get_dag_id_for_integration_invalid_cron(self, service, sample_integration):
        """Test getting DAG ID with invalid cron format."""
        sample_integration.utc_sch_cron = "invalid"

        dag_id = service._get_dag_id_for_integration(sample_integration)
        assert dag_id == "s3_to_mongo_ondemand"

    @patch("control_plane.app.services.integration_service.requests.post")
    def test_trigger_airflow_dag_success(self, mock_post, service):
        """Test triggering Airflow DAG successfully."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"dag_run_id": "manual__2024-01-01T00:00:00"}
        mock_post.return_value = mock_response

        dag_run_id = service._trigger_airflow_dag("s3_to_mongo_ondemand", {"key": "value"})

        assert dag_run_id == "manual__2024-01-01T00:00:00"
        mock_post.assert_called_once()

    @patch("control_plane.app.services.integration_service.requests.post")
    def test_trigger_airflow_dag_failure(self, mock_post, service):
        """Test triggering Airflow DAG with failure."""
        import requests
        mock_post.side_effect = requests.exceptions.RequestException("Connection failed")

        with pytest.raises(Exception) as exc_info:
            service._trigger_airflow_dag("s3_to_mongo_ondemand", {"key": "value"})

        assert "Failed to trigger Airflow DAG" in str(exc_info.value)

    @patch("control_plane.app.services.integration_service.requests.post")
    def test_trigger_dag_run_success(self, mock_post, service, mock_db, sample_integration):
        """Test full trigger_dag_run flow."""
        # Mock get_integration
        mock_query = Mock()
        mock_filter = Mock()
        mock_filter.first.return_value = sample_integration

        mock_query.filter.return_value = mock_filter
        mock_db.query.return_value = mock_query

        # Mock Airflow API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"dag_run_id": "manual__2024-01-01T00:00:00"}
        mock_post.return_value = mock_response

        result = service.trigger_dag_run(1, {"override_key": "override_value"})

        assert result["integration_id"] == 1
        assert result["dag_run_id"] == "manual__2024-01-01T00:00:00"
        assert result["dag_id"] == "s3_to_mongo_daily_02"
        assert result["message"] == "DAG run triggered successfully"

        # Verify IntegrationRun was created
        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()

    def test_trigger_dag_run_integration_not_found(self, service, mock_db):
        """Test triggering DAG run for non-existent integration."""
        # Mock get_integration returning None
        mock_query = Mock()
        mock_filter = Mock()
        mock_filter.first.return_value = None

        mock_query.filter.return_value = mock_filter
        mock_db.query.return_value = mock_query

        with pytest.raises(ValueError) as exc_info:
            service.trigger_dag_run(999)

        assert "Integration 999 not found" in str(exc_info.value)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
