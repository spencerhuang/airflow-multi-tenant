# Dispatcher Pattern for Scheduled DAGs

## Problem

Airflow's scheduler triggers DAG runs with `conf={}`. Our data pipeline operators (e.g., `PrepareS3ToMongoTask`) require configuration like `s3_bucket`, `mongo_collection`, and credentials — all passed via `dag_run.conf`. This means **scheduled DAGs cannot run the pipeline directly**.

On-demand DAGs work because they're triggered via the Airflow REST API with a full `conf` dict (by the control plane service or Kafka consumer). But daily scheduled DAGs have no caller to provide that conf.

## Solution: Dispatcher Pattern

Scheduled DAGs act as lightweight **dispatchers** instead of running the pipeline directly:

```
Airflow Scheduler (cron: 0 2 * * *)
  → daily_02 DAG runs DispatchScheduledIntegrationsTask
    → Queries control plane DB for active integrations due at hour=2
    → For each integration:
      1. Builds conf dict (same as control plane / Kafka consumer)
      2. Creates IntegrationRun record
      3. Triggers s3_to_mongo_ondemand via Airflow REST API
    → Returns summary of dispatched runs
```

Each integration gets its own isolated ondemand DAG run, reusing the existing pipeline (`Prepare → Validate → Execute → Cleanup`) without modification.

## Why This Pattern

| Alternative | Problem |
|---|---|
| Pass conf in scheduled DAG | Scheduler provides `conf={}` — impossible |
| Hardcode config in DAG file | Doesn't scale to thousands of tenants |
| Use Airflow Variables/Connections | Still need per-tenant lookup logic at runtime |
| **Dispatcher (chosen)** | Queries DB at runtime, reuses ondemand pipeline, each integration isolated |

## How It Works

### DispatchScheduledIntegrationsTask

Located in `airflow/plugins/operators/dispatch_operators.py`.

**Constructor args:**
- `schedule_hour` (int): UTC hour to match (0–23)
- `integration_type` (str): e.g., `"s3_to_mongo"`

**Execute flow:**

1. **Find due integrations** — queries `integrations` table:
   ```sql
   WHERE schedule_type = 'daily'
     AND usr_sch_status = 'active'
     AND integration_type = :type
     AND utc_sch_cron IS NOT NULL
   ```
   Then filters by hour in Python (parses cron expression `"0 2 * * *"` → hour 2).

2. **Build conf** — for each integration, merges:
   - Integration fields: `tenant_id`, `integration_id`, `auth_id`, access point IDs
   - `integration.json_data`: non-sensitive workflow config (`s3_bucket`, `s3_prefix`, `mongo_collection`)
   - All `auth` records for the workspace: credentials (`s3_access_key`, `mongo_uri`, etc.)

   This is the same conf-building pattern used by the control plane's `integration_service` and `kafka_consumer_service`.

3. **Trigger ondemand DAG** — POSTs to Airflow REST API:
   ```
   POST /api/v2/dags/s3_to_mongo_ondemand/dagRuns
   {
     "dag_run_id": "<workspace_id>_s3_to_mongo_ondemand_scheduled_<timestamp>",
     "logical_date": "<unique ISO timestamp with microseconds>",
     "conf": { ... merged conf ... }
   }
   ```

4. **Create IntegrationRun** — inserts a tracking record in the control plane DB.

5. **Continue on error** — if one integration fails, logs the error and continues with the rest. Returns a summary with successes and failures.

### DAG Definition

```python
# airflow/dags/s3_to_mongo_daily_02.py
from operators.dispatch_operators import DispatchScheduledIntegrationsTask

with DAG(
    dag_id="s3_to_mongo_daily_02",
    schedule="0 2 * * *",
    max_active_runs=1,
    ...
) as dag:
    dispatch = DispatchScheduledIntegrationsTask(
        task_id="dispatch_integrations",
        schedule_hour=2,
        integration_type="s3_to_mongo",
    )
```

To add more schedule hours, create additional DAGs (e.g., `s3_to_mongo_daily_03`) with `schedule_hour=3`.

## Environment Variables

The dispatcher runs **inside Airflow workers** and needs to:
1. Query the control plane MySQL database
2. Call the Airflow REST API to trigger ondemand DAGs

This requires these environment variables on all Airflow containers:

| Variable | Purpose | Default |
|---|---|---|
| `CONTROL_PLANE_DB_URL` | MySQL connection for querying integrations and auth records | — |
| `AIRFLOW_INTERNAL_API_URL` | Airflow API server URL (internal Docker network) | `http://airflow-apiserver:8080/api/v2` |
| `AIRFLOW_USERNAME` | Credentials for Airflow REST API authentication | `airflow` |
| `AIRFLOW_PASSWORD` | Credentials for Airflow REST API authentication | `airflow` |

**Why does Airflow need its own username/password?**

The dispatcher task runs in an Airflow worker process. To trigger a new DAG run, it calls the Airflow REST API (`POST /dags/{dag_id}/dagRuns`), which requires JWT authentication. The worker authenticates using `AIRFLOW_USERNAME` / `AIRFLOW_PASSWORD` via `get_airflow_auth_headers()` — the same utility the control plane service uses. These are credentials for "self-triggering": an Airflow task calling back into the Airflow API server.

These are set in `docker-compose.yml` under `x-airflow-common > environment` and read by `airflow/config/airflow_config.py` (`ControlPlaneConfig.from_env()`).

## Data Flow Comparison

All three triggering paths build identical conf dicts:

| Trigger Source | When | How |
|---|---|---|
| Control plane API | On-demand / manual | `integration_service.py` → REST API |
| Kafka consumer | CDC event (integration created/updated) | `kafka_consumer_service.py` → REST API |
| **Dispatcher DAG** | Cron schedule | `dispatch_operators.py` → REST API |

All three query the same tables (`integrations`, `auths`) and merge credentials the same way, ensuring consistent behavior regardless of how a DAG run is triggered.

## Testing

### Unit Tests

```bash
pytest airflow/tests/test_s3_to_mongo_operators.py -v -k "dispatch"
```

Covers: successful dispatch, no matching integrations, continue-on-error, hour extraction, hour filtering, missing DB URL.

### E2E Test

```bash
pytest control_plane/tests/test_cron_scheduled_e2e.py -v -s
```

Full pipeline validation:
1. Seeds MySQL with test customer, workspace, auth records, and integration
2. Uploads test data to MinIO
3. Deploys a test dispatcher DAG (`schedule="* * * * *"`)
4. Waits for Airflow scheduler to trigger it
5. Waits for dispatched ondemand DAG to complete
6. Verifies data in MongoDB
7. Verifies IntegrationRun record in MySQL
8. Cleans up all test state

### Airflow 3.0 Notes

- **DAG detection**: Airflow 3.0's dag-processor uses a "bundle" system with refresh intervals. New DAG files aren't detected until the next refresh. The e2e test works around this by restarting the dag-processor after deploying the test DAG.
- **REST API**: `logical_date` is required when creating DAG runs. Use microsecond precision (`%Y-%m-%dT%H:%M:%S.%fZ`) to avoid conflicts when dispatching multiple integrations.
- **API pagination**: `order_by=-queued_at` is not supported; use `order_by=-logical_date` instead.
