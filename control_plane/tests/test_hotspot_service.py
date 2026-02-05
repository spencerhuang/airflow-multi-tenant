"""
Unit tests for Hotspot Analysis Service.

Tests the hotspot identification and forecasting logic for busy-time mitigation
(Spec Section 9.3).
"""

import pytest
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from control_plane.app.services.hotspot_service import HotspotService
from control_plane.app.models.integration import Integration
from control_plane.app.models.workspace import Workspace
from control_plane.app.models.customer import Customer


@pytest.fixture
def sample_integrations(db_session: Session):
    """Create sample integrations for testing."""
    from control_plane.app.models.auth import Auth
    from control_plane.app.models.workflow import Workflow
    from control_plane.app.models.access_point import AccessPoint

    # Create customer and workspace
    customer = Customer(
        customer_guid="test-customer-hotspot",
        name="Test Customer",
        max_integration=1000,
    )
    db_session.add(customer)
    db_session.flush()

    workspace = Workspace(
        workspace_id="test-workspace-hotspot",
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

    # Create integrations with various schedules
    integrations = []

    # Daily integrations at different hours
    for hour in range(24):
        for i in range(10):  # 10 integrations per hour (240 total daily)
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
            integrations.append(integration)
            db_session.add(integration)

    # Weekly integrations (Monday at midnight)
    for i in range(150):
        integration = Integration(
            workspace_id=workspace.workspace_id,
            workflow_id=1,
            auth_id=auth.auth_id,
            source_access_pt_id=1,
            dest_access_pt_id=2,
            integration_type="S3ToMongo",
            schedule_type="weekly",
            usr_sch_cron="0 0 * * 1",  # Monday
            utc_sch_cron="0 0 * * 1",
            usr_timezone="UTC",
            usr_sch_status="active",
        )
        integrations.append(integration)
        db_session.add(integration)

    # Monthly integrations (1st of month at midnight)
    for i in range(50):
        integration = Integration(
            workspace_id=workspace.workspace_id,
            workflow_id=1,
            auth_id=auth.auth_id,
            source_access_pt_id=1,
            dest_access_pt_id=2,
            integration_type="S3ToMongo",
            schedule_type="monthly",
            usr_sch_cron="0 0 1 * *",  # 1st of month
            utc_sch_cron="0 0 1 * *",
            usr_timezone="UTC",
            usr_sch_status="active",
        )
        integrations.append(integration)
        db_session.add(integration)

    db_session.commit()

    return integrations


class TestHotspotService:
    """Tests for HotspotService."""

    @pytest.mark.asyncio
    async def test_analyze_hotspots_basic(self, async_db_session, sample_integrations):
        """Test basic hotspot analysis."""
        start_date = datetime(2026, 2, 3, 0, 0, 0)  # Monday, Feb 3, 2026
        days = 7

        result = await HotspotService.analyze_hotspots(
            db=async_db_session,
            start_date=start_date,
            days=days,
            hotspot_threshold=100,
        )

        # Verify structure
        assert "start_date" in result
        assert "end_date" in result
        assert "hourly_forecast" in result
        assert "hotspots" in result
        assert "statistics" in result

        # Verify date range
        assert result["start_date"] == start_date.isoformat()
        expected_end = start_date + timedelta(days=days)
        assert result["end_date"] == expected_end.isoformat()

        # Verify we have hourly forecast for all hours
        assert len(result["hourly_forecast"]) == days * 24  # 7 days * 24 hours

    @pytest.mark.asyncio
    async def test_monday_first_of_month_convergence(self, async_db_session, sample_integrations):
        """
        Test the worst case: Monday + First of Month.

        This is the scenario from the spec - maximum convergence of schedules.
        """
        # Find a Monday that's also the 1st of month
        # In 2026, June 1st is a Monday
        start_date = datetime(2026, 6, 1, 0, 0, 0)  # Monday, June 1, 2026
        days = 2

        result = await HotspotService.analyze_hotspots(
            db=async_db_session,
            start_date=start_date,
            days=days,
            hotspot_threshold=100,
        )

        # First hour should have ALL schedules
        convergence_hour = result["hourly_forecast"][0]

        assert convergence_hour["breakdown"]["daily"] == 10  # 10 daily at hour 0
        assert convergence_hour["breakdown"]["weekly"] == 150  # 150 weekly (Monday)
        assert convergence_hour["breakdown"]["monthly"] == 50  # 50 monthly (1st)
        assert convergence_hour["total_dag_runs"] == 210  # 10 + 150 + 50

        # Should definitely be a hotspot
        assert convergence_hour["is_hotspot"] is True

        # Check hotspot details
        hotspots = result["hotspots"]
        assert len(hotspots) > 0

        # Should mention Monday AND monthly convergence
        convergence_hotspot = hotspots[0]
        assert "Monday" in convergence_hotspot["reason"] or "monday" in convergence_hotspot["reason"]

        # Should have comprehensive mitigation suggestions
        assert len(convergence_hotspot["mitigation_suggestions"]) >= 3
        assert any("jitter" in s.lower() for s in convergence_hotspot["mitigation_suggestions"])
        assert any("stagger" in s.lower() or "KEDA" in s for s in convergence_hotspot["mitigation_suggestions"])

    @pytest.mark.asyncio
    async def test_statistics_calculation(self, async_db_session, sample_integrations):
        """Test that statistics are calculated correctly."""
        start_date = datetime(2026, 2, 3, 0, 0, 0)
        days = 7

        result = await HotspotService.analyze_hotspots(
            db=async_db_session,
            start_date=start_date,
            days=days,
            hotspot_threshold=100,
        )

        stats = result["statistics"]

        # Verify statistics exist
        assert "total_hours" in stats
        assert "average_runs_per_hour" in stats
        assert "max_runs_per_hour" in stats
        assert "min_runs_per_hour" in stats
        assert "hotspot_hours" in stats
        assert "peak_hour" in stats

        # Verify values make sense
        assert stats["total_hours"] == days * 24
        assert stats["average_runs_per_hour"] > 0
        assert stats["max_runs_per_hour"] >= stats["average_runs_per_hour"]
        assert stats["min_runs_per_hour"] <= stats["average_runs_per_hour"]
