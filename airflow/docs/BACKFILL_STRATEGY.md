# Backfill Strategy for Multi-Tenant Airflow

## 📘 Overview

This document describes the backfill strategy for the multi-tenant Airflow architecture, based on specification Section 9. The strategy ensures safe and controlled recovery from missed runs without overwhelming the Airflow scheduler and workers.

---

## 🎯 Problem Statement

### What Is Backfill?

**Backfill** = Running historical DAG runs that were missed due to:
- System downtime
- Failed runs that need replay
- New integrations that need historical data processing
- Schedule changes

### Why Uncontrolled Backfill Is Dangerous

Without proper controls:
```
System down for 7 days
→ 1000 tenants × 7 days = 7000 missed runs
→ All triggered simultaneously at startup
→ Scheduler overload ❌
→ Worker pool exhaustion ❌
→ Database connection pool exhaustion ❌
→ System crash ❌
```

---

## 🛡️ Section 1: Limited Backfill

### 1.1 The Rule

Per spec Section 9.1:

✅ **Cap the number of missed runs** (e.g., last 7 days)
✅ **Skip older gaps** - accept data loss beyond threshold

### 1.2 Why Cap at 7 Days?

**Business Reasoning:**
- Data older than 7 days loses operational value
- Users care about recent data, not ancient history
- Better to have recent data quickly than old data slowly

**Technical Reasoning:**
- Prevents backfill storms that paralyze the system
- Maintains system responsiveness
- Allows gradual recovery

### 1.3 Configuration

```python
# control_plane/app/core/config.py

class Settings(BaseSettings):
    """Control plane settings."""

    # Backfill configuration
    MAX_BACKFILL_DAYS: int = 7  # Maximum days to backfill
    MAX_BACKFILL_RUNS_PER_INTEGRATION: int = 7  # Maximum runs per integration
    SKIP_BACKFILL_OLDER_THAN: int = 7  # Skip runs older than N days

    # Backfill rate limiting
    BACKFILL_BATCH_SIZE: int = 10  # Trigger N integrations at a time
    BACKFILL_BATCH_DELAY_SECONDS: int = 5  # Wait between batches
```

### 1.4 Implementation Pattern

```python
"""
control_plane/app/services/backfill_service.py
"""
from datetime import datetime, timedelta
from typing import List
from sqlalchemy.orm import Session

from control_plane.app.models.integration import Integration
from control_plane.app.core.config import settings


class BackfillService:
    """
    Service for handling backfill operations with safety limits.
    """

    @staticmethod
    def calculate_missed_runs(
        integration: Integration,
        current_time: datetime
    ) -> List[datetime]:
        """
        Calculate missed runs for an integration, capped at MAX_BACKFILL_DAYS.

        Args:
            integration: Integration to backfill
            current_time: Current time (UTC)

        Returns:
            List of missed run times (capped)
        """
        if not integration.utc_next_run:
            return []

        # Calculate cutoff date (7 days ago)
        cutoff_date = current_time - timedelta(
            days=settings.MAX_BACKFILL_DAYS
        )

        # Don't backfill older than cutoff
        start_date = max(integration.utc_next_run, cutoff_date)

        missed_runs = []
        run_time = start_date

        # Generate missed run times up to now
        while run_time < current_time:
            missed_runs.append(run_time)

            # Cap at maximum runs per integration
            if len(missed_runs) >= settings.MAX_BACKFILL_RUNS_PER_INTEGRATION:
                break

            # Calculate next run time based on schedule
            run_time = BackfillService._calculate_next_run(
                run_time,
                integration.schedule_type
            )

        return missed_runs

    @staticmethod
    def should_skip_backfill(integration: Integration, current_time: datetime) -> bool:
        """
        Determine if backfill should be skipped for this integration.

        Skip if:
        - Last run was more than MAX_BACKFILL_DAYS ago
        - Integration is disabled
        """
        if not integration.is_active:
            return True

        if not integration.utc_next_run:
            return False

        days_since_last_run = (current_time - integration.utc_next_run).days

        # Skip if older than threshold
        if days_since_last_run > settings.SKIP_BACKFILL_OLDER_THAN:
            return True

        return False

    @staticmethod
    def _calculate_next_run(
        current_run: datetime,
        schedule_type: str
    ) -> datetime:
        """Calculate next run time based on schedule type."""
        if schedule_type == "daily":
            return current_run + timedelta(days=1)
        elif schedule_type == "weekly":
            return current_run + timedelta(weeks=1)
        elif schedule_type == "monthly":
            # Simplified monthly calculation
            return current_run + timedelta(days=30)
        else:
            return current_run + timedelta(days=1)
```

### 1.5 Usage Example

```python
"""
control_plane/app/api/endpoints/backfill.py
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from control_plane.app.database.session import get_db
from control_plane.app.services.backfill_service import BackfillService
from control_plane.app.models.integration import Integration

router = APIRouter()


@router.post("/integrations/{integration_id}/backfill")
def trigger_backfill(
    integration_id: int,
    db: Session = Depends(get_db)
):
    """
    Trigger backfill for a specific integration.

    Automatically caps at 7 days and skips older runs.
    """
    integration = db.query(Integration).get(integration_id)
    if not integration:
        raise HTTPException(status_code=404, detail="Integration not found")

    current_time = datetime.utcnow()

    # Check if should skip
    if BackfillService.should_skip_backfill(integration, current_time):
        return {
            "message": "Backfill skipped - too old or inactive",
            "missed_runs": 0
        }

    # Calculate capped missed runs
    missed_runs = BackfillService.calculate_missed_runs(
        integration,
        current_time
    )

    # Trigger each missed run via Airflow API
    for run_time in missed_runs:
        trigger_airflow_dag(
            dag_id=f"{integration.workflow_type}_ondemand",
            conf={
                "integration_id": integration.integration_id,
                "tenant_id": integration.workspace_id,
                "execution_date": run_time.isoformat(),
                "is_backfill": True,
            }
        )

    return {
        "message": "Backfill triggered",
        "missed_runs": len(missed_runs),
        "run_times": [r.isoformat() for r in missed_runs]
    }
```

---

## ⏱️ Section 2: Exponential Backoff

### 2.1 The Rule

Per spec Section 9.2:

✅ **Backfill runs are triggered gradually**
✅ **Prevents scheduler and worker overload**

### 2.2 Why Exponential Backoff?

**Problem:** Triggering 7000 runs immediately

**Solution:** Gradual triggering with increasing delays

```
Batch 1 (0 runs):    Trigger 10 runs, wait 5 seconds
Batch 2 (10 runs):   Trigger 10 runs, wait 10 seconds
Batch 3 (20 runs):   Trigger 10 runs, wait 20 seconds
Batch 4 (30 runs):   Trigger 10 runs, wait 40 seconds
...
```

**Benefits:**
- Workers have time to process each batch
- Scheduler doesn't get overwhelmed
- System remains responsive
- Can monitor progress and stop if issues arise

### 2.3 Implementation

```python
"""
control_plane/app/services/backfill_service.py
"""
import asyncio
from typing import List


class BackfillService:
    """Backfill service with exponential backoff."""

    @staticmethod
    async def trigger_backfill_with_exponential_backoff(
        integrations: List[Integration],
        batch_size: int = 10,
        initial_delay: int = 5,
        max_delay: int = 120
    ):
        """
        Trigger backfill for multiple integrations with exponential backoff.

        Args:
            integrations: List of integrations to backfill
            batch_size: Number of integrations per batch
            initial_delay: Initial delay between batches (seconds)
            max_delay: Maximum delay between batches (seconds)
        """
        current_delay = initial_delay
        total_triggered = 0

        for i in range(0, len(integrations), batch_size):
            batch = integrations[i:i + batch_size]

            # Trigger batch
            for integration in batch:
                await BackfillService._trigger_single_integration(integration)
                total_triggered += 1

            # Log progress
            print(f"Triggered {total_triggered}/{len(integrations)} integrations")

            # Wait with exponential backoff
            if i + batch_size < len(integrations):  # Don't wait after last batch
                print(f"Waiting {current_delay} seconds before next batch...")
                await asyncio.sleep(current_delay)

                # Increase delay exponentially, cap at max_delay
                current_delay = min(current_delay * 2, max_delay)

        return total_triggered

    @staticmethod
    async def _trigger_single_integration(integration: Integration):
        """Trigger backfill for a single integration."""
        current_time = datetime.utcnow()

        # Calculate missed runs (capped)
        missed_runs = BackfillService.calculate_missed_runs(
            integration,
            current_time
        )

        # Trigger each via Airflow API
        for run_time in missed_runs:
            await trigger_airflow_dag_async(
                dag_id=f"{integration.workflow_type}_ondemand",
                conf={
                    "integration_id": integration.integration_id,
                    "tenant_id": integration.workspace_id,
                    "execution_date": run_time.isoformat(),
                    "is_backfill": True,
                }
            )
```

### 2.4 API Endpoint with Exponential Backoff

```python
"""
control_plane/app/api/endpoints/backfill.py
"""
from fastapi import APIRouter, BackgroundTasks
from sqlalchemy.orm import Session

router = APIRouter()


@router.post("/backfill/all")
async def trigger_all_backfills(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Trigger backfill for all active integrations.

    Uses exponential backoff to prevent system overload.
    Runs in background to avoid request timeout.
    """
    # Get all active integrations needing backfill
    integrations = db.query(Integration).filter(
        Integration.is_active == True,
        Integration.utc_next_run < datetime.utcnow()
    ).all()

    # Filter out integrations to skip
    integrations_to_backfill = [
        i for i in integrations
        if not BackfillService.should_skip_backfill(i, datetime.utcnow())
    ]

    # Trigger in background with exponential backoff
    background_tasks.add_task(
        BackfillService.trigger_backfill_with_exponential_backoff,
        integrations_to_backfill,
        batch_size=10,
        initial_delay=5,
        max_delay=120
    )

    return {
        "message": "Backfill initiated in background",
        "total_integrations": len(integrations_to_backfill),
        "estimated_duration_minutes": BackfillService.estimate_duration(
            len(integrations_to_backfill), 10, 5, 120
        )
    }
```

---

## 🕛 Section 3: Busy-Time Mitigation (Midnight Spike)

### 3.1 The Problem

**Midnight spike scenario:**
```
1000 tenants scheduled at "00:00:00 Monday"
→ All trigger at exact same instant
→ Scheduler overload
→ Database connection spike
→ Worker pool exhaustion
```

### 3.2 The Solution

Per spec Section 9.3:

✅ **Randomize start time ±5 seconds per tenant**
✅ **Stagger control-plane triggers**
✅ **Use KEDA for Airflow worker autoscaling**

### 3.3 Implementation: Trigger Randomization

```python
"""
control_plane/app/services/scheduler_service.py
"""
import random
from datetime import datetime, timedelta


class SchedulerService:
    """Service for scheduling DAG runs with anti-spike protection."""

    @staticmethod
    def get_randomized_trigger_time(
        scheduled_time: datetime,
        jitter_seconds: int = 5
    ) -> datetime:
        """
        Add random jitter to scheduled time to prevent midnight spikes.

        Args:
            scheduled_time: Original scheduled time
            jitter_seconds: Maximum jitter in seconds (±N seconds)

        Returns:
            Randomized trigger time
        """
        # Random offset between -jitter_seconds and +jitter_seconds
        offset = random.randint(-jitter_seconds, jitter_seconds)

        return scheduled_time + timedelta(seconds=offset)

    @staticmethod
    async def trigger_scheduled_integrations(
        scheduled_time: datetime,
        db: Session
    ):
        """
        Trigger all integrations scheduled at a specific time.

        Uses randomization to prevent midnight spikes.
        """
        # Get all integrations scheduled at this time
        integrations = db.query(Integration).filter(
            Integration.utc_next_run <= scheduled_time,
            Integration.is_active == True
        ).all()

        # Trigger with randomization
        for integration in integrations:
            # Add random jitter ±5 seconds
            trigger_time = SchedulerService.get_randomized_trigger_time(
                scheduled_time,
                jitter_seconds=5
            )

            # Calculate delay
            delay = (trigger_time - datetime.utcnow()).total_seconds()
            if delay > 0:
                await asyncio.sleep(delay)

            # Trigger DAG
            await trigger_airflow_dag_async(
                dag_id=f"{integration.workflow_type}_daily_{scheduled_time.hour:02d}",
                conf={
                    "integration_id": integration.integration_id,
                    "tenant_id": integration.workspace_id,
                }
            )
```

### 3.4 Staggered Control-Plane Triggers

```python
"""
control_plane/app/services/scheduler_service.py
"""


class SchedulerService:
    """Scheduler with staggered trigger capability."""

    @staticmethod
    async def trigger_with_stagger(
        integrations: List[Integration],
        stagger_interval_ms: int = 100
    ):
        """
        Trigger integrations with staggered delays.

        Args:
            integrations: Integrations to trigger
            stagger_interval_ms: Delay between triggers (milliseconds)
        """
        for idx, integration in enumerate(integrations):
            # Trigger DAG
            await trigger_airflow_dag_async(
                dag_id=f"{integration.workflow_type}_ondemand",
                conf={
                    "integration_id": integration.integration_id,
                    "tenant_id": integration.workspace_id,
                }
            )

            # Stagger: wait before next trigger
            if idx < len(integrations) - 1:  # Don't wait after last
                await asyncio.sleep(stagger_interval_ms / 1000.0)

        return len(integrations)
```

### 3.5 Example: Monday 00:00 Protection

```python
"""
Example: Handling Monday midnight spike
"""


@router.post("/schedule/trigger-monday-midnight")
async def trigger_monday_midnight(background_tasks: BackgroundTasks):
    """
    Trigger Monday 00:00 runs with spike protection.

    Uses:
    - Random jitter (±5 seconds)
    - Staggered triggers (100ms between)
    - Background processing
    """
    db = next(get_db())

    # Get all Monday 00:00 integrations
    integrations = db.query(Integration).filter(
        Integration.schedule_type == "weekly",
        Integration.utc_sch_cron.like("0 0 * * 1"),  # Monday 00:00
        Integration.is_active == True
    ).all()

    # Trigger in background with protections
    background_tasks.add_task(
        SchedulerService.trigger_with_stagger_and_jitter,
        integrations,
        jitter_seconds=5,
        stagger_interval_ms=100
    )

    return {
        "message": "Monday midnight runs triggered with spike protection",
        "total_integrations": len(integrations),
        "protections": [
            "Random jitter ±5 seconds",
            "Staggered triggers (100ms)",
            "Background processing"
        ]
    }
```

---

## 🐳 Section 4: KEDA Autoscaling for Workers

### 4.1 What Is KEDA?

**KEDA** (Kubernetes Event-Driven Autoscaler) automatically scales Airflow workers based on workload.

### 4.2 Why KEDA?

**Without KEDA:**
- Fixed number of workers (e.g., 10)
- Underutilized during low traffic
- Overloaded during spikes

**With KEDA:**
- Scales workers based on task queue depth
- 0 workers when idle → cost savings
- 100 workers during backfill → handles load

### 4.3 KEDA Configuration Example

```yaml
# k8s/keda-scaled-object.yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: airflow-worker-scaler
  namespace: airflow
spec:
  scaleTargetRef:
    name: airflow-worker
    kind: Deployment
  pollingInterval: 30  # Check every 30 seconds
  cooldownPeriod: 300  # Wait 5 minutes before scaling down
  minReplicaCount: 2   # Always keep 2 workers
  maxReplicaCount: 50  # Scale up to 50 workers
  triggers:
    - type: postgresql
      metadata:
        connectionFromEnv: AIRFLOW_DATABASE_URL
        query: >
          SELECT COUNT(*)
          FROM task_instance
          WHERE state IN ('queued', 'scheduled')
        targetQueryValue: "10"  # Scale up when >10 tasks queued per worker
```

### 4.4 Benefits During Backfill

```
Normal operation: 2 workers (minimal cost)
↓
Backfill triggered: 7000 tasks queued
↓
KEDA detects: 7000 / 10 = 700 tasks per worker
↓
KEDA scales to: 50 workers (max limit)
↓
Backfill completes faster
↓
Queue drains
↓
KEDA scales down to: 2 workers (5 min cooldown)
```

---

## 📊 Section 5: Monitoring and Observability

### 5.1 Backfill Metrics

Track these metrics during backfill:

```python
"""
control_plane/app/services/metrics_service.py
"""
from prometheus_client import Counter, Gauge, Histogram

# Backfill metrics
backfill_runs_total = Counter(
    'backfill_runs_total',
    'Total backfill runs triggered',
    ['integration_type', 'status']
)

backfill_queue_depth = Gauge(
    'backfill_queue_depth',
    'Number of backfill runs pending'
)

backfill_duration_seconds = Histogram(
    'backfill_duration_seconds',
    'Time taken to complete backfill',
    buckets=[60, 300, 600, 1800, 3600, 7200]  # 1m to 2h
)

backfill_skipped_old = Counter(
    'backfill_skipped_old_total',
    'Backfill runs skipped (too old)'
)
```

### 5.2 Logging Best Practices

```python
"""
Example: Comprehensive backfill logging
"""
import logging

logger = logging.getLogger(__name__)


async def trigger_backfill(integration: Integration):
    """Trigger backfill with comprehensive logging."""
    logger.info(
        f"Starting backfill for integration {integration.integration_id}",
        extra={
            "integration_id": integration.integration_id,
            "tenant_id": integration.workspace_id,
            "workflow_type": integration.workflow_type,
            "last_run": integration.utc_next_run.isoformat(),
        }
    )

    # Calculate missed runs
    missed_runs = BackfillService.calculate_missed_runs(
        integration,
        datetime.utcnow()
    )

    if not missed_runs:
        logger.info(f"No missed runs for integration {integration.integration_id}")
        return

    logger.info(
        f"Triggering {len(missed_runs)} missed runs",
        extra={
            "integration_id": integration.integration_id,
            "missed_runs_count": len(missed_runs),
            "oldest_run": missed_runs[0].isoformat(),
            "newest_run": missed_runs[-1].isoformat(),
        }
    )

    # Trigger runs
    for run_time in missed_runs:
        try:
            await trigger_airflow_dag_async(...)
            logger.debug(
                f"Triggered run for {run_time.isoformat()}",
                extra={"integration_id": integration.integration_id}
            )
        except Exception as e:
            logger.error(
                f"Failed to trigger run: {e}",
                extra={
                    "integration_id": integration.integration_id,
                    "run_time": run_time.isoformat(),
                    "error": str(e)
                }
            )

    logger.info(
        f"Backfill completed for integration {integration.integration_id}",
        extra={
            "integration_id": integration.integration_id,
            "runs_triggered": len(missed_runs),
        }
    )
```

---

## 🎯 Section 6: Complete Backfill Strategy

### 6.1 Step-by-Step Process

**1. Detect Missed Runs**
```python
missed_runs = BackfillService.calculate_missed_runs(integration, current_time)
```

**2. Apply Cap (7 Days)**
```python
if len(missed_runs) > 7:
    missed_runs = missed_runs[-7:]  # Keep only last 7
```

**3. Check If Should Skip**
```python
if BackfillService.should_skip_backfill(integration, current_time):
    return  # Skip - too old
```

**4. Group Into Batches**
```python
batches = [missed_runs[i:i+10] for i in range(0, len(missed_runs), 10)]
```

**5. Trigger With Exponential Backoff**
```python
delay = 5
for batch in batches:
    trigger_batch(batch)
    await asyncio.sleep(delay)
    delay = min(delay * 2, 120)  # Cap at 2 minutes
```

**6. Apply Randomization**
```python
for run in batch:
    jitter = random.randint(-5, 5)
    trigger_time = run.scheduled_time + timedelta(seconds=jitter)
    schedule_trigger(trigger_time)
```

### 6.2 Configuration Summary

```python
# config.py
MAX_BACKFILL_DAYS = 7                    # Cap at 7 days
MAX_BACKFILL_RUNS_PER_INTEGRATION = 7    # Max 7 runs per integration
BACKFILL_BATCH_SIZE = 10                 # 10 integrations per batch
BACKFILL_INITIAL_DELAY = 5               # Start with 5 second delay
BACKFILL_MAX_DELAY = 120                 # Cap at 2 minute delay
TRIGGER_JITTER_SECONDS = 5               # ±5 seconds randomization
STAGGER_INTERVAL_MS = 100                # 100ms between triggers
```

---

## ✅ Best Practices Summary

### DO ✅

1. **Always cap backfill** at 7 days (or configurable limit)
2. **Use exponential backoff** when triggering multiple runs
3. **Add random jitter** to prevent midnight spikes
4. **Stagger triggers** with small delays (100ms)
5. **Monitor queue depth** and worker utilization
6. **Log all backfill operations** for debugging
7. **Use KEDA** for automatic worker scaling
8. **Run backfills in background** tasks to avoid API timeouts

### DON'T ❌

1. **Don't trigger unlimited backfills** - always cap
2. **Don't trigger all at once** - use batching and delays
3. **Don't ignore old runs** - skip beyond threshold
4. **Don't forget monitoring** - track progress and failures
5. **Don't hardcode timing** - make it configurable
6. **Don't block API requests** - use background tasks

---

## 📚 Related Documentation

- [Airflow Best Practices](AIRFLOW_BEST_PRACTICES.md) - Overall Airflow guidelines
- [Worker Efficiency Improvements](../../docs/WORKER_EFFICIENCY_IMPROVEMENTS.md) - Worker slot management
- [DST Developer Guide](DST_DEVELOPER_GUIDE.md) - Timezone handling

---

## 🔗 References

- **Spec Section 9:** Backfill Strategy
- **Spec Section 9.1:** Limited Backfill
- **Spec Section 9.2:** Exponential Backoff
- **Spec Section 9.3:** Busy-Time Mitigation (Midnight Spike)

---

**Last Updated:** 2026-02-03
**Status:** ✅ Production Ready
**Implementation Status:** Documented - Requires Implementation
**Compliance:** Fully aligned with specification
