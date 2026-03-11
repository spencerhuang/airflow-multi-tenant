"""
Hotspot Analysis API Endpoint

Provides scheduling hotspot analysis to identify busy periods and
support capacity planning and jitter strategy (Spec Section 9.3).
"""

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from control_plane.app.core.database import get_db
from control_plane.app.services.hotspot_service import HotspotService

router = APIRouter()


@router.get("/hotspot", tags=["hotspot"])
async def analyze_hotspots(
    days: int = Query(
        default=10,
        ge=1,
        le=30,
        description="Number of days to forecast (1-30)",
    ),
    threshold: Optional[int] = Query(
        default=None,
        ge=1,
        description="Custom hotspot threshold (default: 100 runs)",
    ),
    start_date: Optional[str] = Query(
        default=None,
        description="Start date in ISO format (default: now UTC)",
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    Analyze scheduling hotspots for capacity planning.

    This endpoint identifies times when many DAG runs will occur simultaneously,
    which is critical for:
    - Busy-time mitigation (Spec Section 9.3)
    - KEDA worker autoscaling planning
    - Applying jitter strategy to high-traffic periods

    **Example Scenario:**
    - Monday 00:00 UTC is:
      - First day of the week (weekly schedules trigger)
      - Potentially first day of month (monthly schedules trigger)
      - Regular daily schedules run
    - Result: 400 daily + 150 weekly + 50 monthly = 600 concurrent DAG runs!

    **Mitigation Strategies Suggested:**
    - Apply ±5 second jitter to randomize trigger times
    - Stagger triggers with 100ms intervals
    - Scale workers with KEDA (2-50 workers based on queue depth)
    - Use exponential backoff for batch triggering

    **Returns:**
    - `hourly_forecast`: Hour-by-hour breakdown for next N days
    - `hotspots`: Identified busy periods with mitigation suggestions
    - `statistics`: Aggregate statistics (avg, max, peak hour)

    **Example Response:**
    ```json
    {
      "start_date": "2026-02-03T00:00:00",
      "end_date": "2026-02-13T00:00:00",
      "total_active_integrations": 1000,
      "hotspot_threshold": 100,
      "hourly_forecast": [
        {
          "timestamp": "2026-02-03T00:00:00",
          "total_dag_runs": 600,
          "breakdown": {
            "daily": 400,
            "weekly": 150,
            "monthly": 50,
            "ondemand_potential": 50
          },
          "is_hotspot": true
        }
      ],
      "hotspots": [
        {
          "timestamp": "2026-02-03T00:00:00",
          "total_dag_runs": 600,
          "reason": "Monday midnight: daily + weekly + monthly convergence",
          "mitigation_suggestions": [
            "Apply ±5 second jitter to randomize triggers",
            "Stagger triggers with 100ms interval",
            "Scale workers to 50+ with KEDA"
          ]
        }
      ],
      "statistics": {
        "average_runs_per_hour": 45.2,
        "max_runs_per_hour": 600,
        "hotspot_hours": 3,
        "peak_hour": "2026-02-03T00:00:00"
      }
    }
    ```
    """
    # Parse start date if provided
    parsed_start_date = None
    if start_date:
        try:
            parsed_start_date = datetime.fromisoformat(
                start_date.replace("Z", "+00:00")
            )
        except ValueError:
            parsed_start_date = None  # Will use default (now)

    # Analyze hotspots
    result = await HotspotService.analyze_hotspots(
        db=db,
        start_date=parsed_start_date,
        days=days,
        hotspot_threshold=threshold,
    )

    return result


@router.get("/hotspot/summary", tags=["hotspot"])
async def get_hotspot_summary(
    days: int = Query(default=7, ge=1, le=30),
    db: AsyncSession = Depends(get_db),
):
    """
    Get a quick summary of upcoming hotspots.

    Returns only the identified hotspots without full hourly breakdown.
    Useful for dashboard widgets and alerts.

    **Returns:**
    ```json
    {
      "next_hotspot": {
        "timestamp": "2026-02-03T00:00:00",
        "total_dag_runs": 600,
        "hours_until": 5.5
      },
      "hotspots_count": 3,
      "highest_load": {
        "timestamp": "2026-02-03T00:00:00",
        "total_dag_runs": 600
      }
    }
    ```
    """
    # Get full analysis
    result = await HotspotService.analyze_hotspots(
        db=db,
        days=days,
    )

    hotspots = result["hotspots"]

    if not hotspots:
        return {
            "next_hotspot": None,
            "hotspots_count": 0,
            "highest_load": None,
        }

    # Find next hotspot (first in chronological order)
    next_hotspot_data = hotspots[0]
    next_hotspot_time = datetime.fromisoformat(
        next_hotspot_data["timestamp"].replace("Z", "+00:00")
    )
    hours_until = (next_hotspot_time - datetime.now(timezone.utc)).total_seconds() / 3600

    # Find highest load
    highest_load = max(hotspots, key=lambda h: h["total_dag_runs"])

    return {
        "next_hotspot": {
            "timestamp": next_hotspot_data["timestamp"],
            "total_dag_runs": next_hotspot_data["total_dag_runs"],
            "hours_until": round(hours_until, 1),
            "reason": next_hotspot_data["reason"],
        },
        "hotspots_count": len(hotspots),
        "highest_load": {
            "timestamp": highest_load["timestamp"],
            "total_dag_runs": highest_load["total_dag_runs"],
            "reason": highest_load["reason"],
        },
        "statistics": result["statistics"],
    }
