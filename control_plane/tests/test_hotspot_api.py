"""
Unit tests for Hotspot API endpoints.

Tests the REST API for hotspot analysis.
"""

import pytest
from datetime import datetime

from control_plane.app.models.integration import Integration
from control_plane.app.models.workspace import Workspace
from control_plane.app.models.customer import Customer


@pytest.fixture
def setup_hotspot_data(db_session):
    """Create sample data for hotspot testing."""
    from control_plane.app.models.auth import Auth
    from control_plane.app.models.workflow import Workflow
    from control_plane.app.models.access_point import AccessPoint

    # Create customer and workspace
    customer = Customer(
        customer_guid="test-customer-api",
        name="Test Customer",
        max_integration=1000,
    )
    db_session.add(customer)
    db_session.flush()

    workspace = Workspace(
        workspace_id="test-workspace-api",
        customer_guid=customer.customer_guid,
    )
    db_session.add(workspace)
    db_session.flush()

    # Create auth
    auth = Auth(
        workspace_id=workspace.workspace_id,
        auth_type="aws_iam",
        json_data='{"access_key": "test"}',
    )
    db_session.add(auth)

    # Create workflow
    workflow = Workflow(workflow_id=1, workflow_type="S3ToMongo")
    db_session.add(workflow)

    # Create access points
    source_ap = AccessPoint(access_pt_id=1, ap_type="S3")
    dest_ap = AccessPoint(access_pt_id=2, ap_type="MongoDB")
    db_session.add_all([source_ap, dest_ap])

    db_session.flush()

    # Create integrations
    # 100 daily at various hours
    for hour in range(24):
        for _ in range(5):
            integration = Integration(
                workspace_id=workspace.workspace_id,
                workflow_id=1,
                auth_id=auth.auth_id,
                source_access_pt_id=1,
                dest_access_pt_id=2,
                integration_type="S3ToMongo",
                schedule_type="daily",
                usr_sch_cron=f"0 {hour} * * *",
                utc_sch_cron=f"0 {hour} * * *",
                usr_timezone="UTC",
                usr_sch_status="active",
            )
            db_session.add(integration)

    # 100 weekly (Monday midnight)
    for _ in range(100):
        integration = Integration(
            workspace_id=workspace.workspace_id,
            workflow_id=1,
            auth_id=auth.auth_id,
            source_access_pt_id=1,
            dest_access_pt_id=2,
            integration_type="S3ToMongo",
            schedule_type="weekly",
            usr_sch_cron="0 0 * * 1",
            utc_sch_cron="0 0 * * 1",
            usr_timezone="UTC",
            usr_sch_status="active",
        )
        db_session.add(integration)

    # 50 monthly (1st of month)
    for _ in range(50):
        integration = Integration(
            workspace_id=workspace.workspace_id,
            workflow_id=1,
            auth_id=auth.auth_id,
            source_access_pt_id=1,
            dest_access_pt_id=2,
            integration_type="S3ToMongo",
            schedule_type="monthly",
            usr_sch_cron="0 0 1 * *",
            utc_sch_cron="0 0 1 * *",
            usr_timezone="UTC",
            usr_sch_status="active",
        )
        db_session.add(integration)

    db_session.commit()


class TestHotspotAPI:
    """Tests for hotspot API endpoints."""

    def test_hotspot_endpoint_basic(self, client, setup_hotspot_data):
        """Test basic hotspot endpoint."""
        response = client.get("/api/v1/hotspot?days=3")

        assert response.status_code == 200

        data = response.json()

        # Verify structure
        assert "start_date" in data
        assert "end_date" in data
        assert "total_active_integrations" in data
        assert "hotspot_threshold" in data
        assert "hourly_forecast" in data
        assert "hotspots" in data
        assert "statistics" in data

        # Verify forecast
        assert len(data["hourly_forecast"]) == 3 * 24  # 3 days * 24 hours

        # Verify each forecast has required fields
        forecast = data["hourly_forecast"][0]
        assert "timestamp" in forecast
        assert "total_dag_runs" in forecast
        assert "breakdown" in forecast
        assert "is_hotspot" in forecast

        # Verify breakdown
        breakdown = forecast["breakdown"]
        assert "daily" in breakdown
        assert "weekly" in breakdown
        assert "monthly" in breakdown
        assert "ondemand_potential" in breakdown

    def test_hotspot_statistics(self, client, setup_hotspot_data):
        """Test that statistics are included in response."""
        response = client.get("/api/v1/hotspot?days=5")

        assert response.status_code == 200

        data = response.json()
        stats = data["statistics"]

        # Verify statistics structure
        assert "total_hours" in stats
        assert "average_runs_per_hour" in stats
        assert "max_runs_per_hour" in stats
        assert "min_runs_per_hour" in stats
        assert "hotspot_hours" in stats
        assert "hotspot_percentage" in stats
        assert "peak_hour" in stats

        # Verify values
        assert stats["total_hours"] == 5 * 24
        assert stats["average_runs_per_hour"] >= 0
        assert stats["max_runs_per_hour"] >= stats["min_runs_per_hour"]
