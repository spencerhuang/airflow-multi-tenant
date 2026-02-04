# Airflow Sensor Best Practices

## 🚨 CRITICAL RULE: Always Use Reschedule Mode

**ALL sensors in this project MUST use `mode='reschedule'`**

This is not optional - it's a hard requirement to prevent worker slot exhaustion.

---

## ❌ NEVER DO THIS

```python
from airflow.sensors.s3_key_sensor import S3KeySensor

# ❌ BAD: Blocks worker slot for entire wait period
sensor = S3KeySensor(
    task_id='wait_for_file',
    bucket_name='my-bucket',
    bucket_key='data/file.json',
    mode='poke',  # DEFAULT - BLOCKS WORKER!
    poke_interval=60,  # Checks every 60 seconds
    timeout=3600,  # Waits up to 1 hour
)

# This worker slot is BLOCKED for up to 1 HOUR!
# With 100 tenants, you'd need 100 worker slots just for sensors!
```

---

## ✅ ALWAYS DO THIS

```python
from airflow.sensors.s3_key_sensor import S3KeySensor

# ✅ GOOD: Frees worker slot between checks
sensor = S3KeySensor(
    task_id='wait_for_file',
    bucket_name='my-bucket',
    bucket_key='data/file.json',
    mode='reschedule',  # REQUIRED - frees worker between checks!
    poke_interval=60,  # Checks every 60 seconds
    timeout=3600,  # Waits up to 1 hour
)

# Worker slot is FREED between checks!
# Task gets rescheduled in the queue every 60 seconds.
```

---

## 📊 Impact Comparison

### Scenario: 100 Tenants Waiting for Files

#### Mode: `poke` (Bad)
```
Worker Slots Needed: 100 (one per tenant)
Worker Utilization: 100% blocked, doing nothing
Other Tasks: Starved, can't run
System Health: ❌ BROKEN
```

#### Mode: `reschedule` (Good)
```
Worker Slots Needed: ~5-10 (for actual checks)
Worker Utilization: Active only during checks
Other Tasks: Run normally
System Health: ✅ HEALTHY
```

---

## 🎯 Sensor Configuration Guidelines

### 1. Required Parameters

```python
sensor = AnySensor(
    task_id='sensor_name',
    mode='reschedule',  # ✅ REQUIRED
    poke_interval=60,   # ✅ REQUIRED - time between checks (seconds)
    timeout=3600,       # ✅ REQUIRED - maximum wait time (seconds)
)
```

### 2. Recommended Poke Intervals

| Frequency | Poke Interval | Use Case |
|-----------|---------------|----------|
| High | 30 seconds | Time-critical operations |
| Medium | 60 seconds | Standard workflows |
| Low | 300 seconds (5 min) | Batch processes |
| Very Low | 600 seconds (10 min) | Large data transfers |

**Rule of thumb:** Longer is better for worker efficiency. Ask yourself: "Does checking every 30 seconds vs 60 seconds really matter for this use case?"

### 3. Reasonable Timeouts

| Timeout | Use Case |
|---------|----------|
| 5 minutes | Quick API responses |
| 30 minutes | Small file uploads |
| 1 hour | Medium data transfers |
| 4 hours | Large batch processes |
| 24 hours | Daily data dumps |

**Important:** Set realistic timeouts. Don't wait indefinitely.

---

## 📚 Common Sensor Types

### S3 Sensor

```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

sensor = S3KeySensor(
    task_id='wait_for_s3_file',
    bucket_name='{{ dag_run.conf["s3_bucket"] }}',
    bucket_key='{{ dag_run.conf["s3_prefix"] }}data.json',
    aws_conn_id='aws_default',
    mode='reschedule',  # ✅ REQUIRED
    poke_interval=60,
    timeout=1800,  # 30 minutes
)
```

### HTTP Sensor

```python
from airflow.providers.http.sensors.http import HttpSensor

sensor = HttpSensor(
    task_id='wait_for_api',
    http_conn_id='external_api',
    endpoint='api/status',
    request_params={'job_id': '{{ ti.xcom_pull("submit_job") }}'},
    response_check=lambda response: response.json()['status'] == 'complete',
    mode='reschedule',  # ✅ REQUIRED
    poke_interval=30,
    timeout=3600,  # 1 hour
)
```

### SQL Sensor

```python
from airflow.providers.common.sql.sensors.sql import SqlSensor

sensor = SqlSensor(
    task_id='wait_for_data',
    conn_id='postgres_default',
    sql='SELECT COUNT(*) FROM staging WHERE loaded_date = CURRENT_DATE',
    success=lambda count: count > 0,
    mode='reschedule',  # ✅ REQUIRED
    poke_interval=300,  # 5 minutes
    timeout=7200,  # 2 hours
)
```

### File Sensor

```python
from airflow.sensors.filesystem import FileSensor

sensor = FileSensor(
    task_id='wait_for_file',
    filepath='/mnt/shared/data/{{ ds }}/complete.flag',
    fs_conn_id='fs_default',
    mode='reschedule',  # ✅ REQUIRED
    poke_interval=60,
    timeout=3600,
)
```

---

## 🎓 Advanced Patterns

### Pattern 1: Sensor with Callback

```python
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.operators.python import PythonOperator

def on_success(**context):
    """Called when sensor succeeds."""
    print(f"File found: {context['task_instance'].xcom_pull('sensor')}")

sensor = S3KeySensor(
    task_id='wait_for_file',
    bucket_name='my-bucket',
    bucket_key='data/file.json',
    mode='reschedule',
    poke_interval=60,
    timeout=3600,
    on_success_callback=on_success,
)
```

### Pattern 2: Sensor with Soft Fail

```python
sensor = S3KeySensor(
    task_id='wait_for_optional_file',
    bucket_name='my-bucket',
    bucket_key='data/optional.json',
    mode='reschedule',
    poke_interval=60,
    timeout=600,  # Wait 10 minutes
    soft_fail=True,  # Don't fail DAG if timeout - mark as skipped
)

# Downstream tasks can check:
# if task_instance.state == 'success':
#     # File exists
# else:
#     # File doesn't exist (soft fail)
```

### Pattern 3: Dynamic Poke Interval

```python
def get_poke_interval(**context):
    """Adjust poke interval based on time of day."""
    from datetime import datetime
    hour = datetime.now().hour
    if 9 <= hour <= 17:  # Business hours
        return 30  # Check every 30 seconds
    else:  # Off-hours
        return 300  # Check every 5 minutes

sensor = S3KeySensor(
    task_id='wait_for_file',
    bucket_name='my-bucket',
    bucket_key='data/file.json',
    mode='reschedule',
    poke_interval="{{ get_poke_interval() }}",  # Dynamic
    timeout=3600,
)
```

---

## ⚠️ Anti-Patterns to Avoid

### Anti-Pattern 1: Sensor in a Loop

```python
# ❌ NEVER DO THIS
from airflow.operators.python import PythonOperator

def wait_and_check():
    import time
    while True:
        if check_condition():
            break
        time.sleep(60)  # Blocks worker for entire duration!

# This is just a poor man's sensor in poke mode
task = PythonOperator(
    task_id='bad_wait',
    python_callable=wait_and_check,
)

# ✅ Use a proper sensor instead
sensor = ConditionSensor(
    task_id='good_wait',
    mode='reschedule',
    poke_interval=60,
)
```

### Anti-Pattern 2: Multiple Sequential Sensors

```python
# ❌ BAD: Multiple sensors in sequence
sensor1 = S3KeySensor(task_id='wait_1', ...)
sensor2 = S3KeySensor(task_id='wait_2', ...)
sensor3 = S3KeySensor(task_id='wait_3', ...)

sensor1 >> sensor2 >> sensor3 >> process

# ✅ BETTER: Use one sensor with pattern matching or list
from airflow.providers.amazon.aws.sensors.s3 import S3KeysUnchangedSensor

sensor = S3KeysUnchangedSensor(
    task_id='wait_for_all',
    bucket_name='my-bucket',
    prefix='data/',
    inactivity_period=300,  # Files haven't changed in 5 minutes
    mode='reschedule',
)

# Or check in a Python task
def check_all_files(**context):
    s3 = S3Hook()
    required_files = ['file1.json', 'file2.json', 'file3.json']
    return all(s3.check_for_key(f'data/{f}', 'my-bucket') for f in required_files)

check = PythonOperator(
    task_id='check_all',
    python_callable=check_all_files,
)
```

### Anti-Pattern 3: Sensor Without Timeout

```python
# ❌ BAD: No timeout - could wait forever
sensor = S3KeySensor(
    task_id='wait_forever',
    bucket_name='my-bucket',
    bucket_key='data/file.json',
    mode='reschedule',
    poke_interval=60,
    # No timeout!
)

# ✅ GOOD: Always set a reasonable timeout
sensor = S3KeySensor(
    task_id='wait_reasonable',
    bucket_name='my-bucket',
    bucket_key='data/file.json',
    mode='reschedule',
    poke_interval=60,
    timeout=3600,  # 1 hour max
)
```

---

## 🔍 Monitoring and Debugging

### Check Sensor Status

```python
# In Airflow UI:
# 1. Go to DAG > Grid View
# 2. Click on sensor task
# 3. Check "Reschedule Date" - shows next check time
# 4. Check "Duration" - should be short if using reschedule mode

# In logs:
[2024-01-15, 10:00:00 UTC] {base.py:73} INFO - Poking callable: <function ...>
[2024-01-15, 10:00:00 UTC] {base.py:229} INFO - Condition not met. Rescheduling.
[2024-01-15, 10:01:00 UTC] {base.py:73} INFO - Poking callable: <function ...>
[2024-01-15, 10:01:00 UTC] {base.py:232} INFO - Success criteria met. Exiting.
```

### Common Issues

#### Issue: "Task not rescheduling"
**Cause:** Using `mode='poke'` instead of `mode='reschedule'`
**Solution:** Change to `mode='reschedule'`

#### Issue: "Sensor timing out too quickly"
**Cause:** Timeout too short for expected wait time
**Solution:** Increase `timeout` parameter

#### Issue: "Too many sensor checks"
**Cause:** `poke_interval` too short
**Solution:** Increase to 60 seconds or more

#### Issue: "Sensor succeeds but file doesn't exist"
**Cause:** Race condition or incorrect logic
**Solution:** Add verification step after sensor

---

## 📋 Sensor Checklist

Before adding a sensor to production, verify:

- [ ] `mode='reschedule'` is set
- [ ] `poke_interval` is reasonable (≥60 seconds recommended)
- [ ] `timeout` is set and appropriate for use case
- [ ] Sensor has meaningful `task_id`
- [ ] Sensor has documentation (`doc_md`)
- [ ] Success criteria is well-defined
- [ ] Failure handling is appropriate (`soft_fail` if needed)
- [ ] Monitoring alerts are configured for timeouts
- [ ] Tested with actual wait times in dev/staging

---

## 🎯 Summary: The One Rule

```python
# Every sensor MUST have this:
mode='reschedule'

# No exceptions. Ever.
```

**Why?**
- Frees worker slots between checks
- Prevents worker exhaustion
- Allows other tasks to run
- Scales to hundreds/thousands of tenants
- Is the only reasonable way to use sensors in production

---

## 📚 Additional Resources

- [Airflow Sensors Official Docs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html)
- [Sensor Mode Comparison](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html#sensor-modes)
- [Available Sensor Operators](https://airflow.apache.org/docs/apache-airflow-providers/)

---

## 🆘 Need Help?

If you're unsure whether to use a sensor:

1. **Ask:** Do I need to wait for an external condition?
   - Yes → Use a sensor
   - No → Use a regular operator

2. **Ask:** Can I check immediately without waiting?
   - Yes → Use a regular operator with retries
   - No → Use a sensor

3. **Ask:** Is the wait time predictable and short (<1 minute)?
   - Yes → Consider using retries instead of a sensor
   - No → Use a sensor

4. **Always:** Use `mode='reschedule'`

---

**Last Updated:** 2026-02-03
**Author:** Airflow Multi-Tenant Architecture Team
**Status:** Production Ready ✅
