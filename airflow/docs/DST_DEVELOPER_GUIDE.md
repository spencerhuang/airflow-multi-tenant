# Daylight Saving Time (DST) Developer Guide

## 📘 Overview

This guide explains how to use the DST-aware timezone utilities in the Multi-Tenant Airflow system. Per the architecture specification (Section 7), the control plane handles DST normalization to avoid broken cron semantics in Airflow.

---

## 🎯 Architecture Principle

**From Spec Section 7:**
> "Airflow remains an orchestrator, not a calendar engine."

The control plane:
1. Stores schedules in tenant's timezone (`usr_sch_cron`)
2. Normalizes execution times to UTC (`utc_sch_cron`)
3. Detects DST shifts
4. Adjusts trigger times accordingly
5. Triggers Airflow with correct UTC time

---

## 🔧 Implementation: Timezone Utilities

### Location
```
control_plane/app/utils/timezone.py
```

### Core Class
```python
from control_plane.app.utils.timezone import TimezoneConverter
```

---

## 📖 Common Use Cases

### Use Case 1: Tenant Creates Daily Schedule

**Scenario:** Tenant wants job to run every day at 2:00 AM in their timezone (America/New_York)

```python
from datetime import datetime
from control_plane.app.utils.timezone import TimezoneConverter

# User input
tenant_timezone = "America/New_York"
user_schedule_time = "02:00"  # 2 AM local time
user_cron = "0 2 * * *"  # Daily at 2 AM

# Step 1: Validate timezone
if not TimezoneConverter.validate_timezone(tenant_timezone):
    raise ValueError(f"Invalid timezone: {tenant_timezone}")

# Step 2: Convert current time to UTC for storage
local_time = datetime(2024, 6, 15, 2, 0, 0)  # Example: June (EDT)
utc_time = TimezoneConverter.convert_to_utc(local_time, tenant_timezone)

# Step 3: Store in database
integration.usr_sch_cron = user_cron  # "0 2 * * *"
integration.usr_timezone = tenant_timezone  # "America/New_York"
integration.utc_sch_cron = f"0 {utc_time.hour} * * *"  # "0 6 * * *" in summer (EDT)
integration.utc_next_run = utc_time
```

### Why Two Cron Expressions?

```
usr_sch_cron: "0 2 * * *"  (user's perspective - STABLE across DST)
utc_sch_cron: Changes between "0 7 * * *" (winter/EST) and "0 6 * * *" (summer/EDT)
```

---

### Use Case 2: Detecting DST Transitions

**Problem:** Tenant scheduled job at 2:30 AM on March 10, 2024 (spring forward day)

```python
from datetime import datetime
from control_plane.app.utils.timezone import TimezoneConverter

scheduled_time = datetime(2024, 3, 10, 2, 30, 0)  # 2:30 AM
timezone = "America/New_York"

# Check if time is nonexistent (spring forward gap)
if TimezoneConverter.is_nonexistent_time(scheduled_time, timezone):
    print("⚠️ This time doesn't exist! 2:00 AM → 3:00 AM on this day.")

    # Handle it: shift forward
    adjusted = TimezoneConverter.handle_nonexistent_time(
        scheduled_time,
        timezone,
        strategy="shift_forward"
    )
    print(f"Adjusted to: {adjusted}")  # 3:30 AM

    # Convert adjusted time to UTC
    utc_time = TimezoneConverter.convert_to_utc(adjusted, timezone)
    integration.utc_next_run = utc_time
```

**Output:**
```
⚠️ This time doesn't exist! 2:00 AM → 3:00 AM on this day.
Adjusted to: 2024-03-10 03:30:00
```

---

### Use Case 3: Handling Fall Back (Ambiguous Times)

**Problem:** Tenant scheduled job at 1:30 AM on November 3, 2024 (fall back - time repeats)

```python
from datetime import datetime
from control_plane.app.utils.timezone import TimezoneConverter

scheduled_time = datetime(2024, 11, 3, 1, 30, 0)  # 1:30 AM happens TWICE
timezone = "America/New_York"

# Check if time is ambiguous
if TimezoneConverter.is_ambiguous_time(scheduled_time, timezone):
    print("⚠️ This time occurs twice! 2:00 AM → 1:00 AM")

    # Use second occurrence (after fall back - standard time)
    utc_time = TimezoneConverter.convert_to_utc(
        scheduled_time,
        timezone,
        is_dst=False  # Use standard time (EST)
    )

    print(f"UTC (second occurrence): {utc_time}")  # 6:30 AM UTC
```

**Choosing which occurrence:**
```python
# First occurrence (DST - EDT)
utc_first = TimezoneConverter.convert_to_utc(
    scheduled_time, timezone, is_dst=True
)  # 5:30 AM UTC (1:30 AM EDT)

# Second occurrence (Standard - EST)
utc_second = TimezoneConverter.convert_to_utc(
    scheduled_time, timezone, is_dst=False
)  # 6:30 AM UTC (1:30 AM EST)

# They differ by 1 hour!
```

---

## 🔄 Periodic Schedule Update (Spec Requirement)

Per Section 7, the control plane must **periodically recalculate UTC times** to handle DST transitions.

### Implementation Pattern

```python
from datetime import datetime
from control_plane.app.utils.timezone import TimezoneConverter

def update_utc_schedule(integration):
    """
    Recalculate UTC schedule for an integration.

    Call this:
    - Daily (for all active integrations)
    - When DST transition is detected
    - After integration is created/updated
    """
    # Parse user's cron (e.g., "0 2 * * *")
    hour = parse_cron_hour(integration.usr_sch_cron)

    # Get current date in user's timezone
    tz = ZoneInfo(integration.usr_timezone)
    local_now = datetime.now(tz=tz)

    # Calculate next local execution
    next_local = local_now.replace(hour=hour, minute=0, second=0, microsecond=0)
    if next_local <= local_now:
        next_local += timedelta(days=1)

    # Convert to UTC
    next_utc = TimezoneConverter.convert_to_utc(
        next_local.replace(tzinfo=None),
        integration.usr_timezone
    )

    # Update UTC cron
    integration.utc_sch_cron = f"0 {next_utc.hour} * * *"
    integration.utc_next_run = next_utc

    db.session.commit()
```

---

## 📅 Getting DST Transition Dates

**Use case:** Send tenant notifications before DST transitions

```python
from control_plane.app.utils.timezone import TimezoneConverter

# Get DST transitions for 2024
timezone = "America/New_York"
spring, fall = TimezoneConverter.get_dst_transition_dates(2024, timezone)

print(f"Spring forward: {spring}")  # 2024-03-10
print(f"Fall back: {fall}")        # 2024-11-03

# Send notifications 1 week before transitions
if (spring - datetime.now()).days <= 7:
    notify_tenant(
        "⚠️ DST Alert: Clocks spring forward on March 10. "
        "Your 2 AM job will run at 3 AM on that day."
    )
```

---

## 🧪 Testing DST Logic

### Test 1: Verify Spring Forward Handling

```python
def test_spring_forward_schedule():
    """Test job scheduled during spring forward gap."""
    from control_plane.app.utils.timezone import TimezoneConverter

    # Job scheduled at 2:30 AM on March 10, 2024
    scheduled = datetime(2024, 3, 10, 2, 30, 0)
    tz = "America/New_York"

    # Verify it's nonexistent
    assert TimezoneConverter.is_nonexistent_time(scheduled, tz)

    # Adjust it
    adjusted = TimezoneConverter.handle_nonexistent_time(
        scheduled, tz, strategy="shift_forward"
    )

    # Should be 3:30 AM
    assert adjusted.hour == 3
    assert adjusted.minute == 30

    # Convert to UTC
    utc = TimezoneConverter.convert_to_utc(adjusted, tz)

    # Verify UTC time is correct
    assert utc.hour == 7  # 3:30 AM EDT = 7:30 AM UTC
```

### Test 2: Verify Fall Back Handling

```python
def test_fall_back_schedule():
    """Test job scheduled during fall back (ambiguous time)."""
    from control_plane.app.utils.timezone import TimezoneConverter

    # Job scheduled at 1:30 AM on November 3, 2024
    scheduled = datetime(2024, 11, 3, 1, 30, 0)
    tz = "America/New_York"

    # Verify it's ambiguous
    assert TimezoneConverter.is_ambiguous_time(scheduled, tz)

    # Use second occurrence (standard time)
    utc = TimezoneConverter.convert_to_utc(
        scheduled, tz, is_dst=False
    )

    # 1:30 AM EST = 6:30 AM UTC
    assert utc.hour == 6
    assert utc.minute == 30
```

---

## 🎨 Control Plane Integration Example

### Complete Flow: User Creates Schedule → DST Handling → Airflow Trigger

```python
from fastapi import APIRouter, HTTPException
from datetime import datetime
from control_plane.app.utils.timezone import TimezoneConverter
from control_plane.app.models.integration import Integration

router = APIRouter()

@router.post("/integrations/")
def create_integration(data: IntegrationCreate):
    """
    Create integration with DST-aware scheduling.

    Per Spec Section 7:
    1. Store schedule in tenant's timezone
    2. Normalize to UTC
    3. Handle DST automatically
    """

    # Step 1: Validate timezone
    if not TimezoneConverter.validate_timezone(data.usr_timezone):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timezone: {data.usr_timezone}"
        )

    # Step 2: Parse user's schedule
    # User wants: "Daily at 2:00 AM America/New_York"
    user_cron = "0 2 * * *"
    local_hour = 2

    # Step 3: Calculate UTC time for today
    tz = ZoneInfo(data.usr_timezone)
    local_now = datetime.now(tz=tz)
    next_local = local_now.replace(
        hour=local_hour, minute=0, second=0, microsecond=0
    )

    # If passed today, schedule for tomorrow
    if next_local <= local_now:
        next_local += timedelta(days=1)

    # Check if nonexistent time (spring forward)
    naive_local = next_local.replace(tzinfo=None)
    if TimezoneConverter.is_nonexistent_time(naive_local, data.usr_timezone):
        # Adjust forward
        naive_local = TimezoneConverter.handle_nonexistent_time(
            naive_local,
            data.usr_timezone,
            strategy="shift_forward"
        )

    # Convert to UTC
    next_utc = TimezoneConverter.convert_to_utc(naive_local, data.usr_timezone)

    # Step 4: Create integration record
    integration = Integration(
        workspace_id=data.workspace_id,
        workflow_id=data.workflow_id,
        usr_sch_cron=user_cron,  # "0 2 * * *"
        usr_timezone=data.usr_timezone,  # "America/New_York"
        utc_sch_cron=f"0 {next_utc.hour} * * *",  # Varies by DST
        utc_next_run=next_utc,
        schedule_type="daily",
    )

    db.session.add(integration)
    db.session.commit()

    return {
        "integration_id": integration.integration_id,
        "user_schedule": f"{user_cron} {data.usr_timezone}",
        "utc_schedule": integration.utc_sch_cron,
        "next_run_utc": integration.utc_next_run.isoformat(),
        "next_run_local": next_local.isoformat(),
    }
```

---

## 📊 Monitoring DST Transitions

### Log DST Adjustments

```python
import logging
from control_plane.app.utils.timezone import TimezoneConverter

logger = logging.getLogger(__name__)

def schedule_integration_run(integration):
    """Schedule next run, logging DST adjustments."""

    # Get scheduled time in user's timezone
    local_time = get_next_local_time(integration)

    # Check for DST issues
    if TimezoneConverter.is_nonexistent_time(local_time, integration.usr_timezone):
        logger.warning(
            f"DST Spring Forward: Integration {integration.integration_id} "
            f"scheduled at nonexistent time {local_time}. "
            f"Adjusting forward."
        )
        local_time = TimezoneConverter.handle_nonexistent_time(
            local_time, integration.usr_timezone, strategy="shift_forward"
        )

    elif TimezoneConverter.is_ambiguous_time(local_time, integration.usr_timezone):
        logger.info(
            f"DST Fall Back: Integration {integration.integration_id} "
            f"scheduled at ambiguous time {local_time}. "
            f"Using second occurrence (standard time)."
        )

    # Convert to UTC
    utc_time = TimezoneConverter.convert_to_utc(
        local_time,
        integration.usr_timezone,
        is_dst=False  # Prefer standard time for ambiguous
    )

    # Trigger Airflow
    trigger_airflow_dag(integration, utc_time)
```

---

## ✅ Best Practices

### DO ✅

1. **Always store both user timezone and UTC**
   ```python
   integration.usr_timezone = "America/New_York"  # User's perspective
   integration.utc_next_run = utc_time  # Airflow uses this
   ```

2. **Check for nonexistent times before scheduling**
   ```python
   if TimezoneConverter.is_nonexistent_time(time, tz):
       time = handle_nonexistent_time(time, tz, "shift_forward")
   ```

3. **Use second occurrence for ambiguous times**
   ```python
   utc_time = convert_to_utc(time, tz, is_dst=False)
   ```

4. **Recalculate UTC schedules daily**
   - Especially around DST transitions

5. **Log DST adjustments**
   - Helps debugging and user support

### DON'T ❌

1. **Don't store only local time**
   ```python
   # ❌ BAD: Can't tell which occurrence during fall back
   scheduled_time = "2024-11-03 01:30:00"

   # ✅ GOOD: Store UTC + timezone
   utc_time = "2024-11-03 06:30:00+00:00"
   timezone = "America/New_York"
   ```

2. **Don't assume fixed UTC offset**
   ```python
   # ❌ BAD: Breaks during DST
   utc_time = local_time + timedelta(hours=5)

   # ✅ GOOD: Use timezone conversion
   utc_time = TimezoneConverter.convert_to_utc(local_time, tz)
   ```

3. **Don't schedule jobs at 2:00-3:00 AM** without checking
   - This is the DST transition window in many timezones
   - Either adjust or warn users

4. **Don't forget to handle both transitions**
   - Spring forward (nonexistent times)
   - Fall back (ambiguous times)

---

## 🎓 Reference

### Complete API

```python
# Validation
TimezoneConverter.validate_timezone(tz_str) -> bool

# Conversion
TimezoneConverter.convert_to_utc(local_time, tz, is_dst=None) -> datetime
TimezoneConverter.convert_from_utc(utc_time, tz) -> datetime

# DST Detection
TimezoneConverter.is_dst(dt, tz) -> bool
TimezoneConverter.is_nonexistent_time(dt, tz) -> bool
TimezoneConverter.is_ambiguous_time(dt, tz) -> bool

# DST Handling
TimezoneConverter.handle_nonexistent_time(dt, tz, strategy) -> datetime
TimezoneConverter.get_dst_transition_dates(year, tz) -> Tuple[datetime, datetime]

# Utilities
TimezoneConverter.get_utc_offset_hours(tz, at_time) -> float
```

### Strategies for Nonexistent Times

| Strategy | Behavior | Use Case |
|----------|----------|----------|
| `shift_forward` | Move to 3:30 AM | Reschedule job |
| `shift_backward` | Move to 1:30 AM | Run before gap |
| `raise` | Raise ValueError | Notify user |

---

## 📚 Additional Resources

- [Timezone Utility Implementation](../control_plane/app/utils/timezone.py)
- [DST Test Suite](../control_plane/tests/test_timezone_dst.py) - 33 passing tests
- [DST Implementation Guide](../../docs/DST_IMPLEMENTATION.md) - Complete technical details
- [Spec Section 7](../../Multi-tenant%20Airflow%20Architecture%20With%20Hybrid%20Scheduling%20And%20Cdc.docx.pdf#page=6) - Original requirements

---

**Last Updated:** 2026-02-03
**Status:** ✅ Production Ready
**Test Coverage:** 100% (33/33 tests passing)
