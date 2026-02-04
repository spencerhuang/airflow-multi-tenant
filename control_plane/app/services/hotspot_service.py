"""
Hotspot Analysis Service

Analyzes scheduling patterns to identify times when many DAG runs will occur
simultaneously (hotspots). This is critical for the busy-time mitigation strategy
described in Spec Section 9.3.

Use cases:
- Identify Monday midnight spike (daily + weekly + monthly convergence)
- Plan capacity (KEDA autoscaling)
- Apply jitter to high-traffic periods
"""

from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass
from collections import defaultdict

from sqlalchemy.orm import Session
from control_plane.app.models.integration import Integration


@dataclass
class HourlyForecast:
    """Forecast for a specific hour."""

    timestamp: datetime
    total_dag_runs: int
    daily_runs: int
    weekly_runs: int
    monthly_runs: int
    ondemand_potential: int  # CDC-triggered (estimated)
    is_hotspot: bool
    hotspot_threshold: int

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON response."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "total_dag_runs": self.total_dag_runs,
            "breakdown": {
                "daily": self.daily_runs,
                "weekly": self.weekly_runs,
                "monthly": self.monthly_runs,
                "ondemand_potential": self.ondemand_potential,
            },
            "is_hotspot": self.is_hotspot,
            "hotspot_threshold": self.hotspot_threshold,
        }


@dataclass
class Hotspot:
    """Identified hotspot period."""

    timestamp: datetime
    total_dag_runs: int
    reason: str
    mitigation_suggestions: List[str]

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON response."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "total_dag_runs": self.total_dag_runs,
            "reason": self.reason,
            "mitigation_suggestions": self.mitigation_suggestions,
        }


class HotspotService:
    """
    Service for analyzing scheduling hotspots.

    Identifies times when many DAG runs will occur simultaneously and
    provides mitigation suggestions.
    """

    # Hotspot threshold: if runs exceed this, it's a hotspot
    DEFAULT_HOTSPOT_THRESHOLD = 100

    # Estimate: on average, X% of integrations might trigger via CDC per hour
    CDC_ONDEMAND_PERCENTAGE = 0.05  # 5% of integrations

    @staticmethod
    def analyze_hotspots(
        db: Session,
        start_date: Optional[datetime] = None,
        days: int = 10,
        hotspot_threshold: Optional[int] = None,
    ) -> Dict:
        """
        Analyze scheduling hotspots for the next N days.

        Args:
            db: Database session
            start_date: Start date (default: now UTC)
            days: Number of days to forecast (default: 10)
            hotspot_threshold: Runs threshold for hotspot (default: 100)

        Returns:
            Dictionary with hourly forecast and identified hotspots
        """
        if start_date is None:
            start_date = datetime.utcnow().replace(
                minute=0, second=0, microsecond=0
            )

        if hotspot_threshold is None:
            hotspot_threshold = HotspotService.DEFAULT_HOTSPOT_THRESHOLD

        end_date = start_date + timedelta(days=days)

        # Get all active integrations
        integrations = (
            db.query(Integration)
            .filter(Integration.usr_sch_status == "active")
            .all()
        )

        # Calculate hourly forecast
        hourly_forecasts = HotspotService._calculate_hourly_forecast(
            integrations, start_date, end_date, hotspot_threshold
        )

        # Identify hotspots
        hotspots = HotspotService._identify_hotspots(hourly_forecasts)

        # Calculate statistics
        stats = HotspotService._calculate_statistics(hourly_forecasts)

        return {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "total_active_integrations": len(integrations),
            "hotspot_threshold": hotspot_threshold,
            "hourly_forecast": [f.to_dict() for f in hourly_forecasts],
            "hotspots": [h.to_dict() for h in hotspots],
            "statistics": stats,
        }

    @staticmethod
    def _calculate_hourly_forecast(
        integrations: List[Integration],
        start_date: datetime,
        end_date: datetime,
        hotspot_threshold: int,
    ) -> List[HourlyForecast]:
        """Calculate forecast for each hour in the date range."""
        forecasts = []
        current_time = start_date

        # Count integrations by schedule type
        daily_by_hour = defaultdict(int)  # hour -> count
        weekly_by_day_hour = defaultdict(int)  # (weekday, hour) -> count
        monthly_by_date_hour = defaultdict(int)  # (day_of_month, hour) -> count

        # Categorize integrations
        for integration in integrations:
            if not integration.usr_sch_cron or not integration.utc_sch_cron:
                continue

            schedule_type = integration.schedule_type

            if schedule_type == "daily":
                # Extract hour from utc_sch_cron (e.g., "0 2 * * *" -> hour 2)
                hour = HotspotService._extract_hour_from_cron(
                    integration.utc_sch_cron
                )
                if hour is not None:
                    daily_by_hour[hour] += 1

            elif schedule_type == "weekly":
                # Extract day of week and hour
                # Format: "0 2 * * 1" (Monday at 2 AM)
                hour = HotspotService._extract_hour_from_cron(
                    integration.utc_sch_cron
                )
                cron_day_of_week = HotspotService._extract_day_of_week_from_cron(
                    integration.utc_sch_cron
                )
                if hour is not None and cron_day_of_week is not None:
                    # Convert cron day-of-week (0=Sunday, 1=Monday) to Python weekday (0=Monday, 6=Sunday)
                    python_weekday = (cron_day_of_week - 1) % 7 if cron_day_of_week != 0 else 6
                    weekly_by_day_hour[(python_weekday, hour)] += 1

            elif schedule_type == "monthly":
                # Extract day of month and hour
                # Format: "0 2 1 * *" (1st of month at 2 AM)
                hour = HotspotService._extract_hour_from_cron(
                    integration.utc_sch_cron
                )
                day_of_month = HotspotService._extract_day_of_month_from_cron(
                    integration.utc_sch_cron
                )
                if hour is not None and day_of_month is not None:
                    monthly_by_date_hour[(day_of_month, hour)] += 1

        # Generate hourly forecast
        while current_time < end_date:
            hour = current_time.hour
            weekday = current_time.weekday()  # 0 = Monday
            day_of_month = current_time.day

            # Count runs for this hour
            daily_runs = daily_by_hour.get(hour, 0)
            weekly_runs = weekly_by_day_hour.get((weekday, hour), 0)
            monthly_runs = monthly_by_date_hour.get((day_of_month, hour), 0)

            # Estimate on-demand (CDC) potential
            total_integrations = len(integrations)
            ondemand_potential = int(
                total_integrations * HotspotService.CDC_ONDEMAND_PERCENTAGE
            )

            total_runs = daily_runs + weekly_runs + monthly_runs

            # Check if hotspot
            is_hotspot = total_runs >= hotspot_threshold

            forecast = HourlyForecast(
                timestamp=current_time,
                total_dag_runs=total_runs,
                daily_runs=daily_runs,
                weekly_runs=weekly_runs,
                monthly_runs=monthly_runs,
                ondemand_potential=ondemand_potential,
                is_hotspot=is_hotspot,
                hotspot_threshold=hotspot_threshold,
            )

            forecasts.append(forecast)
            current_time += timedelta(hours=1)

        return forecasts

    @staticmethod
    def _identify_hotspots(forecasts: List[HourlyForecast]) -> List[Hotspot]:
        """Identify and categorize hotspots with mitigation suggestions."""
        hotspots = []

        for forecast in forecasts:
            if not forecast.is_hotspot:
                continue

            # Determine reason and suggestions
            reasons = []
            suggestions = []

            # Monday midnight check
            if (
                forecast.timestamp.weekday() == 0
                and forecast.timestamp.hour == 0
            ):
                reasons.append(
                    "Monday midnight: daily + weekly + monthly convergence"
                )
                suggestions.extend(
                    [
                        f"Apply ±{5} second jitter to randomize triggers",
                        "Stagger triggers with 100ms interval between integrations",
                        "Scale workers to 50+ with KEDA",
                    ]
                )

            # First of month check
            elif forecast.timestamp.day == 1 and forecast.timestamp.hour == 0:
                reasons.append(
                    "First of month midnight: daily + monthly convergence"
                )
                suggestions.extend(
                    [
                        f"Apply ±{5} second jitter",
                        "Use exponential backoff for batch triggering",
                    ]
                )

            # Monday (any hour) check
            elif forecast.timestamp.weekday() == 0:
                reasons.append("Monday: daily + weekly convergence")
                suggestions.extend(
                    [
                        f"Apply ±{3} second jitter",
                        "Stagger weekly triggers",
                    ]
                )

            # High daily traffic
            elif forecast.daily_runs > forecast.hotspot_threshold * 0.8:
                reasons.append(
                    f"High daily traffic: {forecast.daily_runs} integrations"
                )
                suggestions.extend(
                    [
                        "Consider distributing integrations across more hours",
                        "Apply jitter to prevent exact-time clustering",
                    ]
                )

            # General hotspot
            else:
                reasons.append(f"High traffic: {forecast.total_dag_runs} runs")
                suggestions.append("Apply standard jitter and staggering")

            hotspot = Hotspot(
                timestamp=forecast.timestamp,
                total_dag_runs=forecast.total_dag_runs,
                reason="; ".join(reasons),
                mitigation_suggestions=suggestions,
            )

            hotspots.append(hotspot)

        return hotspots

    @staticmethod
    def _calculate_statistics(forecasts: List[HourlyForecast]) -> Dict:
        """Calculate statistics across all forecasts."""
        if not forecasts:
            return {}

        total_runs = [f.total_dag_runs for f in forecasts]
        hotspot_count = sum(1 for f in forecasts if f.is_hotspot)

        return {
            "total_hours": len(forecasts),
            "average_runs_per_hour": sum(total_runs) / len(total_runs),
            "max_runs_per_hour": max(total_runs),
            "min_runs_per_hour": min(total_runs),
            "hotspot_hours": hotspot_count,
            "hotspot_percentage": (hotspot_count / len(forecasts)) * 100,
            "peak_hour": forecasts[total_runs.index(max(total_runs))].timestamp.isoformat(),
        }

    @staticmethod
    def _extract_hour_from_cron(cron: str) -> Optional[int]:
        """
        Extract hour from cron expression.

        Examples:
            "0 2 * * *" -> 2
            "0 14 * * 1" -> 14
        """
        try:
            parts = cron.split()
            if len(parts) >= 2:
                return int(parts[1])
        except (ValueError, IndexError):
            pass
        return None

    @staticmethod
    def _extract_day_of_week_from_cron(cron: str) -> Optional[int]:
        """
        Extract day of week from cron expression.

        Examples:
            "0 2 * * 1" -> 1 (Monday)
            "0 2 * * 0" -> 0 (Sunday)
        """
        try:
            parts = cron.split()
            if len(parts) >= 5 and parts[4] != "*":
                return int(parts[4])
        except (ValueError, IndexError):
            pass
        return None

    @staticmethod
    def _extract_day_of_month_from_cron(cron: str) -> Optional[int]:
        """
        Extract day of month from cron expression.

        Examples:
            "0 2 1 * *" -> 1 (1st of month)
            "0 2 15 * *" -> 15 (15th of month)
        """
        try:
            parts = cron.split()
            if len(parts) >= 3 and parts[2] != "*":
                return int(parts[2])
        except (ValueError, IndexError):
            pass
        return None
