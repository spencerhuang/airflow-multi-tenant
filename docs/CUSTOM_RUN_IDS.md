# Custom DAG Run IDs for Better Debugging

## Overview

Custom DAG run IDs make it much easier to trace and debug workflow executions by including meaningful context in the run identifier.

## Format

### Manual Triggers (API-triggered)
```
<customer_id>_<dag_id>_manual_<timestamp>
```

**Example:**
```
acme-corp_s3_to_mongo_ondemand_manual_20260204_152345
```

This tells you:
- **Customer:** acme-corp
- **Workflow:** s3_to_mongo_ondemand
- **Type:** manual (API-triggered)
- **Time:** Feb 4, 2026 at 15:23:45

### Scheduled Runs (Future Enhancement)
For scheduled runs managed by Airflow's scheduler, the run ID format is:
```
scheduled__<execution_date>
```

**Example:**
```
scheduled__2026-02-04T00:00:00+00:00
```

**Note:** Airflow's scheduler generates these automatically. Custom IDs for scheduled runs would require modifications to Airflow's core scheduler.

## Implementation

### 1. E2E Test (test_s3_to_mongo_e2e.py)

The E2E test now generates custom run IDs when triggering DAGs:

```python
# Generate custom run_id for easier debugging
timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
custom_run_id = f"test-e2e-cust_{dag_id}_manual_{timestamp}"

trigger_payload = {
    "dag_run_id": custom_run_id,
    "conf": dag_conf,
}
```

**Example output:**
```
test-e2e-cust_s3_to_mongo_ondemand_manual_20260204_152345
```

### 2. Control Plane Integration Service

The integration service automatically generates custom run IDs when triggering DAGs:

**File:** `control_plane/app/services/integration_service.py`

```python
def _trigger_airflow_dag(self, dag_id: str, conf: Dict[str, Any]) -> str:
    # Extract customer/tenant from conf
    tenant_id = conf.get("tenant_id", "unknown")
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")[:17]
    custom_run_id = f"{tenant_id}_{dag_id}_manual_{timestamp}"

    payload = {
        "dag_run_id": custom_run_id,
        "conf": conf,
    }
    # ... trigger API call
```

## Benefits

### 1. **Easy Customer Identification**
```
acme-corp_s3_to_mongo_ondemand_manual_20260204_152345
         ^
         Customer ID - immediately know which tenant
```

### 2. **Workflow Type Visible**
```
acme-corp_s3_to_mongo_ondemand_manual_20260204_152345
                     ^
                     Workflow type - know what integration ran
```

### 3. **Trigger Type Clear**
```
acme-corp_s3_to_mongo_ondemand_manual_20260204_152345
                                ^
                                Manual vs scheduled
```

### 4. **Timestamp for Sorting**
```
acme-corp_s3_to_mongo_ondemand_manual_20260204_152345
                                              ^
                                              Human-readable timestamp
```

## Usage Examples

### Viewing Runs in Airflow UI

**Before (default):**
```
manual__2026-02-04T08:20:12.134621+00:00
manual__2026-02-04T08:21:45.987234+00:00
manual__2026-02-04T08:23:12.456789+00:00
```

❌ Problems:
- Can't tell which customer
- Can't tell what workflow
- All look the same

**After (custom):**
```
acme-corp_s3_to_mongo_ondemand_manual_20260204_082012
widget-inc_s3_to_mongo_ondemand_manual_20260204_082145
acme-corp_azure_to_mongo_ondemand_manual_20260204_082312
```

✅ Benefits:
- Instantly see which customer
- Know the workflow type
- Easy to filter and search

### Filtering in Logs

**Search by customer:**
```bash
# All runs for acme-corp
grep "acme-corp" airflow-scheduler.log

# All S3 to Mongo runs
grep "s3_to_mongo_ondemand" airflow-scheduler.log

# All manual triggers
grep "_manual_" airflow-scheduler.log
```

### Airflow API Queries

**Get specific run:**
```bash
# Old way - cryptic ID
curl http://localhost:8080/api/v1/dags/s3_to_mongo_ondemand/dagRuns/manual__2026-02-04T08:20:12.134621+00:00

# New way - meaningful ID
curl http://localhost:8080/api/v1/dags/s3_to_mongo_ondemand/dagRuns/acme-corp_s3_to_mongo_ondemand_manual_20260204_082012
```

### Database Queries

**Integration runs table:**
```sql
-- Find all runs for a specific customer
SELECT * FROM integration_runs
WHERE dag_run_id LIKE 'acme-corp%'
ORDER BY execution_date DESC;

-- Find all manual runs
SELECT * FROM integration_runs
WHERE dag_run_id LIKE '%_manual_%'
ORDER BY execution_date DESC;

-- Find runs for specific workflow
SELECT * FROM integration_runs
WHERE dag_run_id LIKE '%s3_to_mongo_ondemand%'
ORDER BY execution_date DESC;
```

## Testing

### E2E Test

Run the E2E test to see custom run IDs in action:

```bash
pytest control_plane/tests/test_s3_to_mongo_e2e.py::TestS3ToMongoEndToEnd::test_04_trigger_workflow -v -s
```

**Expected output:**
```
STEP 4: Triggering S3 to MongoDB workflow via Airflow
  Triggering DAG: s3_to_mongo_ondemand
  Custom Run ID: test-e2e-cust_s3_to_mongo_ondemand_manual_20260204_152345
  ✓ DAG triggered successfully
  ✓ DAG Run ID: test-e2e-cust_s3_to_mongo_ondemand_manual_20260204_152345
```

### Control Plane API

Trigger via API and see custom run IDs:

```bash
curl -X POST http://localhost:8000/api/v1/integrations/trigger \
  -H "Content-Type: application/json" \
  -d '{
    "integration_id": 1,
    "execution_config": {}
  }'
```

**Response:**
```json
{
  "integration_id": 1,
  "dag_run_id": "workspace-123_s3_to_mongo_ondemand_manual_20260204_152345",
  "dag_id": "s3_to_mongo_ondemand",
  "message": "DAG run triggered successfully"
}
```

### Verify in Airflow UI

1. Open http://localhost:8080
2. Click on `s3_to_mongo_ondemand` DAG
3. View "DAG Runs" tab
4. See custom run IDs instead of generic `manual__` IDs

## Troubleshooting

### Issue: Run ID Already Exists

If you trigger the same integration twice in the same second:

```
Error: DAG run with ID 'acme-corp_s3_to_mongo_ondemand_manual_20260204_152345' already exists
```

**Solution:**
The timestamp includes microseconds (truncated to milliseconds) to avoid collisions:
```python
timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")[:17]
# Result: 20260204_152345_1
```

### Issue: Run ID Too Long

Airflow has a limit on run ID length (typically 250 characters).

**Current format length:**
```
acme-corp_s3_to_mongo_ondemand_manual_20260204_152345
         10  +  25  +  7  +  15  = ~57 characters
```

This is well within the limit. However, if you have very long customer IDs:

**Solution:**
Truncate or hash the customer ID if needed:
```python
tenant_id = conf.get("tenant_id", "unknown")[:30]  # Limit to 30 chars
```

## Future Enhancements

### 1. Include Integration ID

```
acme-corp_int-12345_s3_to_mongo_ondemand_manual_20260204_152345
```

Benefits:
- Direct link to integration record
- Easier database lookups

### 2. Include Run Type

```
acme-corp_s3_to_mongo_ondemand_backfill_20260204_152345
acme-corp_s3_to_mongo_ondemand_cdc_20260204_152345
acme-corp_s3_to_mongo_ondemand_manual_20260204_152345
```

Benefits:
- Distinguish between backfills, CDC triggers, and manual runs

### 3. Include Source/Destination

```
acme-corp_s3-prod_mongo-analytics_ondemand_manual_20260204_152345
```

Benefits:
- Know which systems are involved
- Easier troubleshooting

## Best Practices

### 1. Always Use Custom Run IDs

When triggering DAGs programmatically, always provide a custom run ID:

```python
# Good
trigger_payload = {
    "dag_run_id": f"{customer_id}_{dag_id}_manual_{timestamp}",
    "conf": config
}

# Bad
trigger_payload = {
    "conf": config  # Airflow generates generic ID
}
```

### 2. Include Context in Run ID

Make the run ID as informative as possible without making it too long:

```python
# Good - clear and informative
f"{customer}_{workflow}_manual_{timestamp}"

# Too short - missing context
f"run_{timestamp}"

# Too long - unnecessary detail
f"{customer}_{workspace}_{integration}_{workflow}_{source}_{dest}_{type}_{timestamp}"
```

### 3. Use Consistent Format

Stick to the same format across all integrations for easier parsing:

```python
# Consistent
<entity>_<workflow>_<type>_<timestamp>

# Examples
acme-corp_s3_to_mongo_ondemand_manual_20260204_152345
widget-inc_azure_to_s3_ondemand_manual_20260204_152346
```

## Summary

Custom run IDs transform debugging from:
```
"Which run was that? Let me search by timestamp..."
```

To:
```
"Oh, it's acme-corp, S3 to Mongo, manual trigger at 15:23 - found it!"
```

This simple change dramatically improves operational efficiency and reduces time to resolution for issues.

---

**Status:** ✅ Implemented
**Version:** 1.0
**Last Updated:** 2026-02-04
