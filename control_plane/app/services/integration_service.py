"""Integration service for business logic related to integrations."""

import asyncio
from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
import requests
from requests.auth import HTTPBasicAuth

from control_plane.app.models.integration import Integration
from control_plane.app.models.integration_run import IntegrationRun
from control_plane.app.schemas.integration import IntegrationCreate, IntegrationUpdate
from control_plane.app.core.config import settings
from control_plane.app.core.retry import DB_RETRY, DB_TIMEOUT
from control_plane.app.utils.timezone import TimezoneConverter


class IntegrationService:
    """
    Service for managing integrations and triggering DAG runs.

    This service handles:
    - CRUD operations for integrations
    - Triggering Airflow DAG runs via REST API
    - Recording integration run history
    """

    def __init__(self, db: AsyncSession):
        """
        Initialize integration service with async database session.

        Args:
            db: Async database session
        """
        self.db = db

    @DB_RETRY
    async def create_integration(self, integration_data: IntegrationCreate) -> Integration:
        """
        Create a new integration with DST-aware UTC conversion.

        Per DST_DEVELOPER_GUIDE.md, this method:
        1. Validates the user's timezone
        2. Stores the user's schedule in their timezone (usr_sch_cron)
        3. Converts to UTC and stores (utc_sch_cron)
        4. Calculates next run time in UTC (utc_next_run)

        Args:
            integration_data: Integration creation data

        Returns:
            Created integration instance

        Raises:
            ValueError: If timezone is invalid or schedule cannot be parsed
        """
        async def _execute():
            # Step 1: Validate timezone
            user_timezone = integration_data.usr_timezone or "UTC"
            if not TimezoneConverter.validate_timezone(user_timezone):
                raise ValueError(f"Invalid timezone: {user_timezone}")

            # Step 2: Calculate UTC schedule if cron is provided
            utc_sch_cron = None
            utc_next_run = None

            if integration_data.usr_sch_cron and integration_data.schedule_type != "on_demand":
                # Parse hour from user's cron (format: "0 H * * *" or "0 H * * D")
                hour, day_of_week, day_of_month = self._parse_cron_schedule(
                    integration_data.usr_sch_cron
                )

                if hour is not None:
                    # Get current time in user's timezone
                    tz = ZoneInfo(user_timezone)
                    local_now = datetime.now(tz=tz)

                    # Calculate next local execution time
                    next_local = local_now.replace(
                        hour=hour, minute=0, second=0, microsecond=0
                    )

                    # Adjust for schedule type
                    if integration_data.schedule_type == "weekly" and day_of_week is not None:
                        # Find next occurrence of this day of week
                        days_ahead = (day_of_week - local_now.weekday()) % 7
                        if days_ahead == 0 and next_local <= local_now:
                            days_ahead = 7
                        next_local += timedelta(days=days_ahead)
                    elif integration_data.schedule_type == "monthly" and day_of_month is not None:
                        # Find next occurrence of this day of month
                        next_local = next_local.replace(day=day_of_month)
                        if next_local <= local_now:
                            # Move to next month
                            if next_local.month == 12:
                                next_local = next_local.replace(year=next_local.year + 1, month=1)
                            else:
                                next_local = next_local.replace(month=next_local.month + 1)
                    elif next_local <= local_now:
                        # For daily, if time has passed today, schedule for tomorrow
                        next_local += timedelta(days=1)

                    # Check for nonexistent time (spring forward)
                    naive_local = next_local.replace(tzinfo=None)
                    if TimezoneConverter.is_nonexistent_time(naive_local, user_timezone):
                        # Adjust forward
                        naive_local = TimezoneConverter.handle_nonexistent_time(
                            naive_local, user_timezone, strategy="shift_forward"
                        )

                    # Convert to UTC
                    next_utc = TimezoneConverter.convert_to_utc(naive_local, user_timezone)

                    # Build UTC cron expression
                    utc_hour = next_utc.hour
                    if integration_data.schedule_type == "daily":
                        utc_sch_cron = f"0 {utc_hour} * * *"
                    elif integration_data.schedule_type == "weekly":
                        # Convert Python weekday (0=Monday) to cron day (0=Sunday, 1=Monday)
                        utc_weekday = next_utc.weekday()
                        cron_day = (utc_weekday + 1) % 7 if utc_weekday != 6 else 0
                        utc_sch_cron = f"0 {utc_hour} * * {cron_day}"
                    elif integration_data.schedule_type == "monthly":
                        utc_day = next_utc.day
                        utc_sch_cron = f"0 {utc_hour} {utc_day} * *"

                    utc_next_run = next_utc

            # Step 3: Create integration with both user and UTC schedules
            integration = Integration(
                workspace_id=integration_data.workspace_id,
                workflow_id=integration_data.workflow_id,
                auth_id=integration_data.auth_id,
                source_access_pt_id=integration_data.source_access_pt_id,
                dest_access_pt_id=integration_data.dest_access_pt_id,
                integration_type=integration_data.integration_type,
                usr_sch_cron=integration_data.usr_sch_cron,  # User's perspective (display)
                usr_timezone=user_timezone,
                utc_sch_cron=utc_sch_cron,  # Airflow uses this
                utc_next_run=utc_next_run,  # Airflow uses this
                schedule_type=integration_data.schedule_type,
                json_data=integration_data.json_data,
                usr_sch_status="active",
            )

            self.db.add(integration)
            await self.db.commit()
            await self.db.refresh(integration)

            return integration
        
        return await asyncio.wait_for(_execute(), timeout=DB_TIMEOUT)

    @DB_RETRY
    async def get_integration(self, integration_id: int) -> Optional[Integration]:
        """
        Get integration by ID.

        Args:
            integration_id: Integration identifier

        Returns:
            Integration instance or None if not found
        """
        async def _execute():
            result = await self.db.execute(
                select(Integration).where(Integration.integration_id == integration_id)
            )
            return result.scalars().first()
        
        return await asyncio.wait_for(_execute(), timeout=DB_TIMEOUT)

    @DB_RETRY
    async def list_integrations(
        self, workspace_id: Optional[str] = None, skip: int = 0, limit: int = 100
    ) -> List[Integration]:
        """
        List integrations with optional workspace filter.

        Args:
            workspace_id: Optional workspace identifier to filter by
            skip: Number of records to skip
            limit: Maximum number of records to return

        Returns:
            List of integrations
        """
        async def _execute():
            query = select(Integration)

            if workspace_id:
                query = query.where(Integration.workspace_id == workspace_id)

            query = query.offset(skip).limit(limit)
            result = await self.db.execute(query)
            return list(result.scalars().all())
        
        return await asyncio.wait_for(_execute(), timeout=DB_TIMEOUT)

    @DB_RETRY
    async def update_integration(
        self, integration_id: int, update_data: IntegrationUpdate
    ) -> Optional[Integration]:
        """
        Update an existing integration with DST-aware UTC recalculation.

        If timezone or schedule changes, recalculates UTC values.

        Args:
            integration_id: Integration identifier
            update_data: Update data

        Returns:
            Updated integration instance or None if not found

        Raises:
            ValueError: If timezone is invalid
        """
        async def _execute():
            integration = await self.get_integration(integration_id)
            if not integration:
                return None

            update_dict = update_data.model_dump(exclude_unset=True)

            # Check if timezone or schedule changed
            schedule_changed = (
                "usr_sch_cron" in update_dict
                or "usr_timezone" in update_dict
                or "schedule_type" in update_dict
            )

            # Apply updates
            for key, value in update_dict.items():
                setattr(integration, key, value)

            # Recalculate UTC values if schedule changed
            if schedule_changed and integration.usr_sch_cron and integration.schedule_type != "on_demand":
                # Validate timezone
                user_timezone = integration.usr_timezone or "UTC"
                if not TimezoneConverter.validate_timezone(user_timezone):
                    raise ValueError(f"Invalid timezone: {user_timezone}")

                # Parse hour from user's cron
                hour, day_of_week, day_of_month = self._parse_cron_schedule(
                    integration.usr_sch_cron
                )

                if hour is not None:
                    # Get current time in user's timezone
                    tz = ZoneInfo(user_timezone)
                    local_now = datetime.now(tz=tz)

                    # Calculate next local execution time
                    next_local = local_now.replace(
                        hour=hour, minute=0, second=0, microsecond=0
                    )

                    # Adjust for schedule type
                    if integration.schedule_type == "weekly" and day_of_week is not None:
                        days_ahead = (day_of_week - local_now.weekday()) % 7
                        if days_ahead == 0 and next_local <= local_now:
                            days_ahead = 7
                        next_local += timedelta(days=days_ahead)
                    elif integration.schedule_type == "monthly" and day_of_month is not None:
                        next_local = next_local.replace(day=day_of_month)
                        if next_local <= local_now:
                            if next_local.month == 12:
                                next_local = next_local.replace(year=next_local.year + 1, month=1)
                            else:
                                next_local = next_local.replace(month=next_local.month + 1)
                    elif next_local <= local_now:
                        next_local += timedelta(days=1)

                    # Check for nonexistent time (spring forward)
                    naive_local = next_local.replace(tzinfo=None)
                    if TimezoneConverter.is_nonexistent_time(naive_local, user_timezone):
                        naive_local = TimezoneConverter.handle_nonexistent_time(
                            naive_local, user_timezone, strategy="shift_forward"
                        )

                    # Convert to UTC
                    next_utc = TimezoneConverter.convert_to_utc(naive_local, user_timezone)

                    # Build UTC cron expression
                    utc_hour = next_utc.hour
                    if integration.schedule_type == "daily":
                        integration.utc_sch_cron = f"0 {utc_hour} * * *"
                    elif integration.schedule_type == "weekly":
                        utc_weekday = next_utc.weekday()
                        cron_day = (utc_weekday + 1) % 7 if utc_weekday != 6 else 0
                        integration.utc_sch_cron = f"0 {utc_hour} * * {cron_day}"
                    elif integration.schedule_type == "monthly":
                        utc_day = next_utc.day
                        integration.utc_sch_cron = f"0 {utc_hour} {utc_day} * *"

                    integration.utc_next_run = next_utc

            await self.db.commit()
            await self.db.refresh(integration)

            return integration
        
        return await asyncio.wait_for(_execute(), timeout=DB_TIMEOUT)

    @DB_RETRY
    async def delete_integration(self, integration_id: int) -> bool:
        """
        Delete an integration.

        Args:
            integration_id: Integration identifier

        Returns:
            True if deleted, False if not found
        """
        async def _execute():
            integration = await self.get_integration(integration_id)
            if not integration:
                return False

            await self.db.delete(integration)
            await self.db.commit()

            return True
        
        return await asyncio.wait_for(_execute(), timeout=DB_TIMEOUT)

    @DB_RETRY
    async def trigger_dag_run(
        self, integration_id: int, execution_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Trigger an Airflow DAG run for an integration.

        This method:
        1. Retrieves integration configuration
        2. Constructs DAG run configuration
        3. Calls Airflow REST API to trigger DAG
        4. Records integration run in database

        Args:
            integration_id: Integration identifier
            execution_config: Optional override configuration for this run

        Returns:
            Dictionary containing dag_run_id and status

        Raises:
            ValueError: If integration not found
            Exception: If Airflow API call fails
        """
        async def _execute():
            integration = await self.get_integration(integration_id)
            if not integration:
                raise ValueError(f"Integration {integration_id} not found")

            # Construct DAG ID based on workflow type and schedule
            # For on-demand runs, use the _ondemand DAG
            dag_id = self._get_dag_id_for_integration(integration)

            # Prepare configuration for DAG run
            conf = {
                "tenant_id": integration.workspace_id,
                "integration_id": integration.integration_id,
                "integration_type": integration.integration_type,
                "auth_id": integration.auth_id,
                "source_access_pt_id": integration.source_access_pt_id,
                "dest_access_pt_id": integration.dest_access_pt_id,
            }

            # Merge integration json_data
            if integration.json_data:
                try:
                    json_config = json.loads(integration.json_data)
                    conf.update(json_config)
                except json.JSONDecodeError:
                    pass

            # Override with execution_config if provided
            if execution_config:
                conf.update(execution_config)

            # Trigger DAG via Airflow REST API (synchronous)
            dag_run_id = self._trigger_airflow_dag(dag_id, conf)

            # Record integration run
            integration_run = IntegrationRun(
                integration_id=integration_id,
                dag_run_id=dag_run_id,
                execution_date=datetime.utcnow(),
            )
            self.db.add(integration_run)
            await self.db.commit()

            return {
                "integration_id": integration_id,
                "dag_run_id": dag_run_id,
                "dag_id": dag_id,
                "message": "DAG run triggered successfully",
            }
        
        return await asyncio.wait_for(_execute(), timeout=DB_TIMEOUT)

    def _parse_cron_schedule(self, cron: str) -> tuple[Optional[int], Optional[int], Optional[int]]:
        """
        Parse cron expression to extract hour, day of week, and day of month.

        Args:
            cron: Cron expression (e.g., "0 2 * * *", "0 2 * * 1", "0 2 15 * *")

        Returns:
            Tuple of (hour, day_of_week, day_of_month)
            - hour: 0-23
            - day_of_week: 0-6 for cron (0=Sunday, 1=Monday, ..., 6=Saturday), None if "*"
            - day_of_month: 1-31, None if "*"
        """
        try:
            parts = cron.split()
            if len(parts) < 5:
                return (None, None, None)

            hour = int(parts[1]) if parts[1] != "*" else None
            day_of_month = int(parts[2]) if parts[2] != "*" else None
            day_of_week = int(parts[4]) if parts[4] != "*" else None

            return (hour, day_of_week, day_of_month)
        except (ValueError, IndexError):
            return (None, None, None)

    def _get_dag_id_for_integration(self, integration: Integration) -> str:
        """
        Determine the DAG ID to use for an integration.

        IMPORTANT: Uses UTC cron (utc_sch_cron) not user cron (usr_sch_cron).
        Airflow DAGs are scheduled in UTC, so we must use the UTC hour.

        For daily schedules, maps to hourly DAGs based on UTC hour.
        For all other schedules (weekly, monthly, on_demand, cdc), uses _ondemand DAG.

        Args:
            integration: Integration instance

        Returns:
            DAG identifier
        """
        # Convert integration_type to snake_case for DAG naming
        workflow_name = integration.integration_type.lower().replace("to", "_to_")

        if integration.schedule_type == "daily" and integration.utc_sch_cron:
            # Extract hour from UTC cron (format: "0 H * * *")
            # This is the UTC hour when Airflow will run the DAG
            try:
                cron_parts = integration.utc_sch_cron.split()
                if len(cron_parts) >= 2:
                    hour = cron_parts[1].zfill(2)
                    return f"{workflow_name}_daily_{hour}"
            except (IndexError, ValueError):
                pass

        # Default to on-demand DAG (for weekly, monthly, on_demand, cdc)
        return f"{workflow_name}_ondemand"

    def _trigger_airflow_dag(self, dag_id: str, conf: Dict[str, Any]) -> str:
        """
        Trigger Airflow DAG via REST API with custom run ID.

        Args:
            dag_id: DAG identifier
            conf: Configuration to pass to DAG run

        Returns:
            DAG run ID

        Raises:
            Exception: If API call fails
        """
        url = f"{settings.AIRFLOW_API_URL}/dags/{dag_id}/dagRuns"

        # Generate custom run_id for easier debugging
        # Format: <customer_id>_<dag_id>_manual_<timestamp>
        # Extract customer/tenant from conf if available
        tenant_id = conf.get("tenant_id", "unknown")
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")[:17]  # YYYYmmdd_HHMMSS
        custom_run_id = f"{tenant_id}_{dag_id}_manual_{timestamp}"

        payload = {
            "dag_run_id": custom_run_id,
            "conf": conf,
        }

        try:
            response = requests.post(
                url,
                json=payload,
                auth=HTTPBasicAuth(settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD),
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            response.raise_for_status()

            result = response.json()
            return result.get("dag_run_id")

        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to trigger Airflow DAG {dag_id}: {str(e)}")
