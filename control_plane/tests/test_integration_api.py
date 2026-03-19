"""Unit tests for Integration API endpoints."""

import pytest
from unittest.mock import patch, Mock

from control_plane.app.models.customer import Customer
from control_plane.app.models.workspace import Workspace
from control_plane.app.models.auth import Auth
from control_plane.app.models.workflow import Workflow
from control_plane.app.models.access_point import AccessPoint


@pytest.fixture
def sample_data(db_session):
    """Create sample data for testing."""
    # Create customer
    customer = Customer(customer_guid="cust-123", name="Test Customer", max_integration=100)
    db_session.add(customer)

    # Create workspace
    workspace = Workspace(workspace_id="ws-123", customer_guid="cust-123")
    db_session.add(workspace)

    # Create auth
    auth = Auth(
        workspace_id="ws-123",
        auth_type="aws_iam",
        json_data='{"access_key": "test"}',
    )
    db_session.add(auth)

    # Create workflow
    workflow = Workflow(workflow_id=1, workflow_type="S3ToMongo")
    db_session.add(workflow)

    # Create access points
    s3_ap = AccessPoint(access_pt_id=1, ap_type="S3")
    mongo_ap = AccessPoint(access_pt_id=2, ap_type="MongoDB")
    db_session.add_all([s3_ap, mongo_ap])

    db_session.commit()

    return {
        "workspace_id": "ws-123",
        "workflow_id": 1,
        "auth_id": auth.auth_id,
        "source_access_pt_id": 1,
        "dest_access_pt_id": 2,
    }


class TestHealthEndpoints:
    """Test health check endpoints."""

    def test_health_check(self, client):
        """Test basic health check endpoint."""
        response = client.get("/api/v1/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "version" in data


class TestIntegrationEndpoints:
    """Test Integration CRUD endpoints."""

    def test_create_integration(self, client, sample_data):
        """Test creating a new integration."""
        integration_data = {
            **sample_data,
            "integration_type": "S3ToMongo",
            "usr_sch_cron": "0 2 * * *",
            "usr_timezone": "America/New_York",
            "schedule_type": "daily",
            "json_data": '{"bucket": "test-bucket"}',
        }

        response = client.post("/api/v1/integrations/", json=integration_data)
        assert response.status_code == 201
        data = response.json()
        assert data["integration_type"] == "S3ToMongo"
        assert data["usr_sch_status"] == "active"
        assert "integration_id" in data

    def test_list_integrations(self, client, sample_data):
        """Test listing integrations."""
        # Create an integration first
        integration_data = {
            **sample_data,
            "integration_type": "S3ToMongo",
            "schedule_type": "daily",
        }
        client.post("/api/v1/integrations/", json=integration_data)

        # List integrations
        response = client.get("/api/v1/integrations/")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) > 0

    def test_get_integration(self, client, sample_data):
        """Test getting a specific integration."""
        # Create an integration
        integration_data = {
            **sample_data,
            "integration_type": "S3ToMongo",
            "schedule_type": "daily",
        }
        create_response = client.post("/api/v1/integrations/", json=integration_data)
        integration_id = create_response.json()["integration_id"]

        # Get the integration
        response = client.get(f"/api/v1/integrations/{integration_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["integration_id"] == integration_id
        assert data["integration_type"] == "S3ToMongo"

    def test_get_integration_not_found(self, client):
        """Test getting non-existent integration."""
        response = client.get("/api/v1/integrations/99999")
        assert response.status_code == 404

    def test_update_integration(self, client, sample_data):
        """Test updating an integration."""
        # Create an integration
        integration_data = {
            **sample_data,
            "integration_type": "S3ToMongo",
            "schedule_type": "daily",
        }
        create_response = client.post("/api/v1/integrations/", json=integration_data)
        integration_id = create_response.json()["integration_id"]

        # Update the integration
        update_data = {
            "usr_sch_status": "paused",
            "usr_sch_cron": "0 3 * * *",
        }
        response = client.put(f"/api/v1/integrations/{integration_id}", json=update_data)
        assert response.status_code == 200
        data = response.json()
        assert data["usr_sch_status"] == "paused"
        assert data["usr_sch_cron"] == "0 3 * * *"

    def test_delete_integration(self, client, sample_data):
        """Test deleting an integration."""
        # Create an integration
        integration_data = {
            **sample_data,
            "integration_type": "S3ToMongo",
            "schedule_type": "daily",
        }
        create_response = client.post("/api/v1/integrations/", json=integration_data)
        integration_id = create_response.json()["integration_id"]

        # Delete the integration
        response = client.delete(f"/api/v1/integrations/{integration_id}")
        assert response.status_code == 204

        # Verify it's deleted
        get_response = client.get(f"/api/v1/integrations/{integration_id}")
        assert get_response.status_code == 404

    @patch("shared_utils.dag_trigger.get_airflow_auth_headers")
    @patch("shared_utils.dag_trigger.requests.post")
    def test_trigger_integration(self, mock_post, mock_auth_headers, client, sample_data):
        """Test triggering an integration."""
        mock_auth_headers.return_value = {"Authorization": "Bearer token", "Content-Type": "application/json"}
        # Mock Airflow API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"dag_run_id": "manual__2024-01-01T00:00:00"}
        mock_post.return_value = mock_response

        # Create an integration
        integration_data = {
            **sample_data,
            "integration_type": "S3ToMongo",
            "schedule_type": "on_demand",
        }
        create_response = client.post("/api/v1/integrations/", json=integration_data)
        integration_id = create_response.json()["integration_id"]

        # Trigger the integration
        trigger_data = {
            "integration_id": integration_id,
            "execution_config": {"backfill_date": "2024-01-01"},
        }
        response = client.post("/api/v1/integrations/trigger", json=trigger_data)
        assert response.status_code == 200
        data = response.json()
        assert "dag_run_id" in data
        assert data["integration_id"] == integration_id


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
