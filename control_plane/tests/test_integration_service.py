"""Unit tests for Integration Service."""

import pytest
import pytest_asyncio
from unittest.mock import Mock, patch, MagicMock, AsyncMock

from control_plane.app.services.integration_service import IntegrationService
from control_plane.app.models.integration import Integration
from control_plane.app.schemas.integration import IntegrationCreate, IntegrationUpdate


class TestIntegrationService:
    """Test IntegrationService class."""

    @pytest_asyncio.fixture
    async def mock_db(self):
        """
        Fixture for mocked async database session.

        We use AsyncMock for the session itself (so methods like commit/execute
        are awaitable), and regular MagicMock for the Result objects returned
        from execute(), to match SQLAlchemy's async Session/Result behaviour:
        - await session.execute(...) -> Result
        - result.scalars().first() / .all() -> plain Python objects
        """
        db = AsyncMock()
        return db

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

    @pytest.mark.asyncio
    async def test_create_integration(self, service, mock_db, sample_integration_create):
        """Test creating a new integration."""
        await service.create_integration(sample_integration_create)

        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()
        mock_db.refresh.assert_called_once()

        added_integration = mock_db.add.call_args[0][0]
        assert added_integration.workspace_id == "ws-123"
        assert added_integration.integration_type == "S3ToMongo"
        assert added_integration.usr_sch_status == "active"

    @pytest.mark.asyncio
    async def test_get_integration_found(self, service, mock_db, sample_integration):
        """Test getting an integration that exists."""
        # Simulate: result.scalars().first() -> sample_integration
        mock_result = MagicMock()
        scalar_result = MagicMock()
        scalar_result.first.return_value = sample_integration
        mock_result.scalars.return_value = scalar_result

        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await service.get_integration(1)

        assert result == sample_integration
        mock_db.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_integration_not_found(self, service, mock_db):
        """Test getting an integration that doesn't exist."""
        mock_result = MagicMock()
        scalar_result = MagicMock()
        scalar_result.first.return_value = None
        mock_result.scalars.return_value = scalar_result

        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await service.get_integration(999)

        assert result is None

    @pytest.mark.asyncio
    async def test_list_integrations_no_filter(self, service, mock_db, sample_integration):
        """Test listing all integrations without workspace filter."""
        mock_result = MagicMock()
        scalar_result = MagicMock()
        scalar_result.all.return_value = [sample_integration]
        mock_result.scalars.return_value = scalar_result

        mock_db.execute = AsyncMock(return_value=mock_result)

        results = await service.list_integrations()

        assert len(results) == 1
        assert results[0] == sample_integration
        mock_db.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_list_integrations_with_workspace_filter(self, service, mock_db):
        """Test listing integrations with workspace filter."""
        mock_result = MagicMock()
        scalar_result = MagicMock()
        scalar_result.all.return_value = []
        mock_result.scalars.return_value = scalar_result

        mock_db.execute = AsyncMock(return_value=mock_result)

        await service.list_integrations(workspace_id="ws-123", skip=10, limit=50)

        mock_db.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_integration_success(self, service, mock_db, sample_integration):
        """Test updating an integration successfully."""
        update_data = IntegrationUpdate(
            usr_sch_status="paused",
            usr_sch_cron="0 3 * * *",
        )

        # Mock get_integration -> sample_integration
        mock_result = MagicMock()
        scalar_result = MagicMock()
        scalar_result.first.return_value = sample_integration
        mock_result.scalars.return_value = scalar_result

        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await service.update_integration(1, update_data)

        assert result == sample_integration
        assert sample_integration.usr_sch_status == "paused"
        assert sample_integration.usr_sch_cron == "0 3 * * *"
        mock_db.commit.assert_called_once()
        mock_db.refresh.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_integration_not_found(self, service, mock_db):
        """Test updating a non-existent integration."""
        update_data = IntegrationUpdate(usr_sch_status="paused")

        # Mock get_integration returning None
        mock_result = MagicMock()
        scalar_result = MagicMock()
        scalar_result.first.return_value = None
        mock_result.scalars.return_value = scalar_result

        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await service.update_integration(999, update_data)

        assert result is None
        mock_db.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_delete_integration_success(self, service, mock_db, sample_integration):
        """Test deleting an integration successfully."""
        # Mock get_integration
        mock_result = MagicMock()
        scalar_result = MagicMock()
        scalar_result.first.return_value = sample_integration
        mock_result.scalars.return_value = scalar_result

        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await service.delete_integration(1)

        assert result is True
        mock_db.delete.assert_called_once_with(sample_integration)
        mock_db.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_integration_not_found(self, service, mock_db):
        """Test deleting a non-existent integration."""
        # Mock get_integration returning None
        mock_result = MagicMock()
        scalar_result = MagicMock()
        scalar_result.first.return_value = None
        mock_result.scalars.return_value = scalar_result

        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await service.delete_integration(999)

        assert result is False
        mock_db.delete.assert_not_called()

    def test_determine_dag_id_daily(self, service, sample_integration):
        """Test determining DAG ID for daily integration via shared function.

        All schedule types now return _ondemand — the Controller DAG
        handles scheduled dispatching via DTM.
        """
        from shared_utils import determine_dag_id
        dag_id = determine_dag_id(
            sample_integration.integration_type,
            sample_integration.schedule_type,
            sample_integration.utc_sch_cron,
        )
        assert dag_id == "s3_to_mongo_ondemand"

    def test_determine_dag_id_ondemand(self, service, sample_integration):
        """Test determining DAG ID for on-demand integration."""
        from shared_utils import determine_dag_id
        dag_id = determine_dag_id(
            sample_integration.integration_type,
            "on_demand",
            None,
        )
        assert dag_id == "s3_to_mongo_ondemand"

    def test_determine_dag_id_invalid_cron(self, service, sample_integration):
        """Test determining DAG ID with invalid cron format."""
        from shared_utils import determine_dag_id
        dag_id = determine_dag_id(
            sample_integration.integration_type,
            sample_integration.schedule_type,
            "invalid",
        )
        assert dag_id == "s3_to_mongo_ondemand"

    @patch("shared_utils.dag_trigger.get_airflow_auth_headers")
    @patch("shared_utils.dag_trigger.requests.post")
    def test_trigger_airflow_dag_success(self, mock_post, mock_auth_headers, service):
        """Test triggering Airflow DAG successfully via shared function."""
        from shared_utils import trigger_airflow_dag
        mock_auth_headers.return_value = {"Authorization": "Bearer token", "Content-Type": "application/json"}
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"dag_run_id": "manual__2024-01-01T00:00:00"}
        mock_post.return_value = mock_response

        dag_run_id = trigger_airflow_dag(
            "http://localhost:8080/api/v2", "airflow", "airflow",
            "s3_to_mongo_ondemand", {"tenant_id": "test", "key": "value"},
        )

        assert dag_run_id == "manual__2024-01-01T00:00:00"
        mock_post.assert_called_once()

    @patch("shared_utils.dag_trigger.get_airflow_auth_headers")
    @patch("shared_utils.dag_trigger.requests.post")
    def test_trigger_airflow_dag_failure(self, mock_post, mock_auth_headers, service):
        """Test triggering Airflow DAG with failure via shared function."""
        import requests
        from shared_utils import trigger_airflow_dag
        mock_auth_headers.return_value = {"Authorization": "Bearer token", "Content-Type": "application/json"}
        mock_post.side_effect = requests.exceptions.RequestException("Connection failed")

        with pytest.raises(requests.exceptions.RequestException):
            trigger_airflow_dag(
                "http://localhost:8080/api/v2", "airflow", "airflow",
                "s3_to_mongo_ondemand", {"tenant_id": "test", "key": "value"},
            )

    @pytest.mark.asyncio
    @patch("shared_utils.dag_trigger.get_airflow_auth_headers")
    @patch("shared_utils.dag_trigger.requests.post")
    async def test_trigger_dag_run_success(self, mock_post, mock_auth_headers, service, mock_db, sample_integration):
        """Test full trigger_dag_run flow."""
        mock_auth_headers.return_value = {"Authorization": "Bearer token", "Content-Type": "application/json"}

        # Mock get_integration
        mock_result = MagicMock()
        scalar_result = MagicMock()
        scalar_result.first.return_value = sample_integration
        mock_result.scalars.return_value = scalar_result

        mock_db.execute = AsyncMock(return_value=mock_result)

        # Mock Airflow API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"dag_run_id": "manual__2024-01-01T00:00:00"}
        mock_post.return_value = mock_response

        result = await service.trigger_dag_run(1, {"override_key": "override_value"})

        assert result["integration_id"] == 1
        assert result["dag_run_id"] == "manual__2024-01-01T00:00:00"
        assert result["dag_id"] == "s3_to_mongo_ondemand"
        assert result["message"] == "DAG run triggered successfully"

        # Verify IntegrationRun was created
        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_trigger_dag_run_integration_not_found(self, service, mock_db):
        """Test triggering DAG run for non-existent integration."""
        # Mock get_integration returning None
        mock_result = MagicMock()
        scalar_result = MagicMock()
        scalar_result.first.return_value = None
        mock_result.scalars.return_value = scalar_result

        mock_db.execute = AsyncMock(return_value=mock_result)

        with pytest.raises(ValueError) as exc_info:
            await service.trigger_dag_run(999)

        assert "Integration 999 not found" in str(exc_info.value)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
