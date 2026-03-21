

# Kubernetes Deployment Guide

## 📘 Overview

This guide explains how to deploy the Multi-Tenant Airflow system to Kubernetes with full configuration externalization via ConfigMaps and Secrets.

**Key Features:**
- ✅ All configuration externalized to environment variables
- ✅ Kubernetes ConfigMaps for non-sensitive configuration
- ✅ Kubernetes Secrets for sensitive data
- ✅ KEDA autoscaling for Airflow workers
- ✅ HPA for control plane scaling
- ✅ Production-ready health checks and resource limits
- ✅ HA Scheduler with PodDisruptionBudget (Airflow 3.0 native multi-scheduler)
- ✅ Separate DAG Processor deployment (Airflow 3.0 architecture)
- ✅ Redis Sentinel HA (3 data nodes + 3 sentinels, automatic failover)

---

## 🎯 Architecture Principles

### Configuration Hierarchy

```
Environment Variables (K8s ConfigMaps/Secrets)
    ↓
Application Configuration Modules
    ↓
    ├─→ airflow/config/airflow_config.py  (DAG configuration)
    └─→ control_plane/app/core/config.py  (Control plane configuration)
        ↓
    Application Runtime (DAGs, APIs, Services)
```

### Configuration Sources

1. **ConfigMaps** - Non-sensitive configuration
   - DAG concurrency settings
   - Retry policies
   - Backfill parameters
   - Service endpoints

2. **Secrets** - Sensitive data
   - Database passwords
   - API credentials
   - JWT secrets
   - Cloud provider keys

3. **Fallback Defaults** - Hardcoded in config modules
   - Used when environment variables not set
   - Sensible defaults for development

---

## 📂 Directory Structure

```
k8s/
├── configmaps/
│   ├── airflow-config.yaml           # Airflow DAG configuration
│   └── control-plane-config.yaml     # Control plane configuration
├── deployments/
│   ├── airflow-worker-deployment.yaml
│   ├── airflow-scheduler-deployment.yaml    # HA (2 replicas + PDB)
│   ├── airflow-dag-processor-deployment.yaml # Airflow 3.0 DAG parsing
│   ├── redis-sentinel-statefulset.yaml      # Redis HA (3 data + 3 sentinel)
│   └── control-plane-deployment.yaml
├── secrets/
│   └── example-secrets.yaml          # Example (DO NOT use in production)
└── K8S_DEPLOYMENT_GUIDE.md          # This file
```

---

## 🚀 Quick Start

### Prerequisites

- Kubernetes cluster (v1.19+)
- kubectl configured
- Helm (optional, for easier deployment)
- KEDA installed (optional, for worker autoscaling)

### Step 1: Create Namespace

```bash
kubectl create namespace airflow
```

### Step 2: Apply ConfigMaps

```bash
# Apply Airflow configuration
kubectl apply -f k8s/configmaps/airflow-config.yaml

# Apply Control Plane configuration
kubectl apply -f k8s/configmaps/control-plane-config.yaml
```

### Step 3: Create Secrets

**⚠️ IMPORTANT:** Do NOT use the example secrets in production!

```bash
# Create secrets (use proper secret management in production)
kubectl create secret generic airflow-secrets \
  --from-literal=AIRFLOW_PASSWORD='changeme' \
  --from-literal=POSTGRES_PASSWORD='changeme' \
  --namespace=airflow

kubectl create secret generic control-plane-secrets \
  --from-literal=DATABASE_PASSWORD='changeme' \
  --from-literal=SECRET_KEY='change-this-to-random-value' \
  --from-literal=MINIO_SECRET_KEY='changeme' \
  --namespace=airflow
```

### Step 4: Deploy Applications

```bash
# Deploy Redis Sentinel HA (must be up before Airflow components)
kubectl apply -f k8s/deployments/redis-sentinel-statefulset.yaml

# Deploy Control Plane
kubectl apply -f k8s/deployments/control-plane-deployment.yaml

# Deploy Airflow Scheduler (HA - 2 replicas with PodDisruptionBudget)
kubectl apply -f k8s/deployments/airflow-scheduler-deployment.yaml

# Deploy Airflow DAG Processor (Airflow 3.0 - separate DAG parsing service)
kubectl apply -f k8s/deployments/airflow-dag-processor-deployment.yaml

# Deploy Airflow Workers
kubectl apply -f k8s/deployments/airflow-worker-deployment.yaml
```

### Step 5: Verify Deployment

```bash
# Check pod status
kubectl get pods -n airflow

# Check configuration was loaded
kubectl logs -n airflow deployment/airflow-worker | grep "Airflow Configuration"

# Check control plane logs
kubectl logs -n airflow deployment/control-plane
```

---

## ⚙️ Configuration Reference

### Airflow DAG Configuration

All Airflow DAG settings are configured via the `airflow-config` ConfigMap.

#### Concurrency Settings

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `AIRFLOW_MAX_ACTIVE_RUNS` | 100 | Max concurrent DAG instances (daily) |
| `AIRFLOW_MAX_ACTIVE_TASKS` | 30 | Max concurrent tasks per DAG run |
| `AIRFLOW_MAX_ACTIVE_RUNS_ONDEMAND` | 50 | Max concurrent on-demand DAGs |

**Example:**
```yaml
# k8s/configmaps/airflow-config.yaml
data:
  AIRFLOW_MAX_ACTIVE_RUNS: "100"        # Increased for production
  AIRFLOW_MAX_ACTIVE_TASKS: "30"       # More tasks per run
  AIRFLOW_MAX_ACTIVE_RUNS_ONDEMAND: "50"  # Higher CDC capacity
```

#### Retry Settings

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `AIRFLOW_RETRIES` | 3 | Number of retry attempts |
| `AIRFLOW_RETRY_DELAY_MINUTES` | 1 | Initial retry delay |
| `AIRFLOW_RETRY_EXPONENTIAL_BACKOFF` | True | Enable exponential backoff |
| `AIRFLOW_MAX_RETRY_DELAY_MINUTES` | 15 | Max retry delay (cap) |

**Example:**
```yaml
data:
  AIRFLOW_RETRIES: "5"                       # More retry attempts
  AIRFLOW_RETRY_DELAY_MINUTES: "2"           # Start with 2 minutes
  AIRFLOW_RETRY_EXPONENTIAL_BACKOFF: "True"  # 2, 4, 8, 16, 32 minutes
  AIRFLOW_MAX_RETRY_DELAY_MINUTES: "30"      # Cap at 30 minutes
```

#### Sensor Settings (if sensors are used)

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `AIRFLOW_SENSOR_MODE` | reschedule | Sensor mode (reschedule/poke) |
| `AIRFLOW_SENSOR_POKE_INTERVAL_SECONDS` | 60 | Check interval |
| `AIRFLOW_SENSOR_TIMEOUT_SECONDS` | 3600 | Timeout (1 hour) |

**⚠️ CRITICAL:** Always use `mode='reschedule'` to prevent worker blocking!

### Control Plane Configuration

All control plane settings are configured via the `control-plane-config` ConfigMap.

#### Backfill Settings

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `MAX_BACKFILL_DAYS` | 7 | Maximum days to backfill |
| `MAX_BACKFILL_RUNS_PER_INTEGRATION` | 7 | Max runs per integration |
| `BACKFILL_BATCH_SIZE` | 10 | Integrations per batch |
| `BACKFILL_BATCH_DELAY_SECONDS` | 5 | Delay between batches |
| `BACKFILL_JITTER_SECONDS` | 5 | Random jitter (±N seconds) |

**Example:**
```yaml
data:
  MAX_BACKFILL_DAYS: "14"                    # Backfill up to 2 weeks
  BACKFILL_BATCH_SIZE: "20"                  # Larger batches
  BACKFILL_INITIAL_DELAY_SECONDS: "10"       # Longer initial delay
```

#### DST Settings

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `DEFAULT_TIMEZONE` | UTC | Default timezone for schedules |
| `DST_TRANSITION_CHECK_ENABLED` | True | Enable DST checks |

**Example:**
```yaml
data:
  DEFAULT_TIMEZONE: "America/New_York"       # Eastern Time
  DST_TRANSITION_CHECK_ENABLED: "True"       # Always enable
```

---

## 🔐 Secrets Management

### Development (Local Testing)

For local development, you can use simple Kubernetes secrets:

```bash
kubectl create secret generic my-secrets \
  --from-literal=KEY1=value1 \
  --from-literal=KEY2=value2 \
  --namespace=airflow
```

### Production (Recommended Approaches)

#### Option 1: External Secrets Operator

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: airflow-secrets
  namespace: airflow
spec:
  secretStoreRef:
    name: aws-secrets-manager
    kind: SecretStore
  target:
    name: airflow-secrets
  data:
    - secretKey: AIRFLOW_PASSWORD
      remoteRef:
        key: airflow/password
    - secretKey: DATABASE_PASSWORD
      remoteRef:
        key: database/password
```

#### Option 2: HashiCorp Vault

```yaml
apiVersion: secrets-store.csi.x-k8s.io/v1
kind: SecretProviderClass
metadata:
  name: vault-airflow
  namespace: airflow
spec:
  provider: vault
  parameters:
    vaultAddress: "https://vault.example.com"
    roleName: "airflow"
    objects: |
      - objectName: "airflow-password"
        secretPath: "secret/data/airflow/password"
        secretKey: "password"
```

#### Option 3: Sealed Secrets

```bash
# Install sealed-secrets controller
kubectl apply -f https://github.com/bitnami-labs/sealed-secrets/releases/download/v0.18.0/controller.yaml

# Create sealed secret
kubectl create secret generic my-secret \
  --from-literal=password=changeme \
  --dry-run=client -o yaml | \
  kubeseal -o yaml > sealed-secret.yaml

# Apply sealed secret
kubectl apply -f sealed-secret.yaml
```

---

## 📊 Resource Configuration

### Airflow Workers

```yaml
resources:
  requests:
    cpu: "500m"       # 0.5 CPU cores
    memory: "1Gi"     # 1 GB RAM
  limits:
    cpu: "2000m"      # 2 CPU cores
    memory: "4Gi"     # 4 GB RAM
```

**Recommendations:**
- **Development:** requests: 250m CPU, 512Mi RAM
- **Production:** requests: 500m-1000m CPU, 1-2Gi RAM
- **High Volume:** requests: 1000m-2000m CPU, 2-4Gi RAM

### Control Plane

```yaml
resources:
  requests:
    cpu: "250m"       # 0.25 CPU cores
    memory: "512Mi"   # 512 MB RAM
  limits:
    cpu: "1000m"      # 1 CPU core
    memory: "2Gi"     # 2 GB RAM
```

---

## 🔄 Autoscaling

### KEDA for Airflow Workers

KEDA (Kubernetes Event-Driven Autoscaling) scales workers using two triggers. KEDA scales to the **maximum** of all triggers, so workers ramp up proactively before tasks are even queued.

| Trigger | Source | Query | Purpose |
|---------|--------|-------|---------|
| `postgresql` | Airflow metadata DB | `task_instance WHERE state IN ('queued','scheduled')` | **Reactive** — scales when tasks are already queued |
| `mysql` | Business DB | `integrations WHERE schedule_type != 'on_demand' AND utc_next_run BETWEEN NOW() AND +24h` | **Proactive** — scales before tasks are created |

The MySQL trigger counts scheduled integrations (daily/weekly/monthly) due in the next 24 hours, excluding on-demand integrations (CDC-triggered, unpredictable). Each integration creates ~4 task instances, so 5 integrations per worker is a good ratio.

#### Prerequisites

```bash
# Install KEDA
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda --namespace keda --create-namespace
```

#### Configuration

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: airflow-worker-scaler
spec:
  scaleTargetRef:
    name: airflow-worker
  minReplicaCount: 2    # Always 2 workers minimum
  maxReplicaCount: 50   # Scale up to 50 workers
  pollingInterval: 30   # Check every 30 seconds
  cooldownPeriod: 300   # Wait 5 min before scaling down
  triggers:
    # Reactive: Airflow task queue depth
    - type: postgresql
      metadata:
        query: "SELECT COUNT(*) FROM task_instance WHERE state IN ('queued', 'scheduled')"
        targetQueryValue: "10"
    # Proactive: scheduled integrations due in next 24h
    - type: mysql
      metadata:
        query: >
          SELECT COUNT(*) FROM integrations
          WHERE usr_sch_status = 'active'
            AND schedule_type != 'on_demand'
            AND utc_next_run BETWEEN NOW() AND DATE_ADD(NOW(), INTERVAL 24 HOUR)
        targetQueryValue: "5"
```

> **⚠️ IMPORTANT — Raw SQL dependency:** The KEDA MySQL trigger queries `integrations` directly. If the table schema changes (column renames, table renames), the KEDA query in `k8s/deployments/airflow-worker-deployment.yaml` must be updated manually. See the [Schema Change Checklist](#schema-change-checklist) below.

**Benefits:**
- Proactive scaling: workers spin up before dispatcher creates tasks
- Reactive fallback: task queue depth handles CDC-triggered on-demand spikes
- Cost savings: scales down after 5-minute cooldown

### HPA for Control Plane

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: control-plane-hpa
spec:
  scaleTargetRef:
    name: control-plane
  minReplicas: 3
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          averageUtilization: 70
```

---

## 🔁 HA Scheduler (Airflow 3.0)

Airflow 3.0 supports native High Availability scheduling. Multiple scheduler instances coordinate via database row-level locks — no external lock manager (Redis/ZooKeeper) required.

### Architecture

| Component | Replicas | Purpose |
|-----------|----------|---------|
| **Scheduler** | 2 (HA) | Picks up and schedules task instances. DB row-level locks prevent duplicate scheduling. |
| **DAG Processor** | 1 | Parses DAG files and writes serialized DAGs to the database. Separated from scheduler in Airflow 3.0. |

### Key Configuration

| Setting | Value | Purpose |
|---------|-------|---------|
| `AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK` | `True` | Exposes `/health` on port 8974 for k8s probes |
| `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` | `http://airflow-apiserver:8080/execution/` | Task SDK communication (Airflow 3.0) |
| Scheduler replicas | 2 | HA via DB row-level locking |
| PodDisruptionBudget | minAvailable: 1 | Protects during node drains / cluster upgrades |

### Verify HA Scheduler

```bash
# Check scheduler pods are running (expect 2)
kubectl get pods -n airflow -l component=scheduler

# Check dag-processor is running (expect 1)
kubectl get pods -n airflow -l component=dag-processor

# Verify PDB is active
kubectl get pdb -n airflow

# Check scheduler health endpoint
kubectl exec -n airflow deployment/airflow-scheduler -- curl -s http://localhost:8974/health
```

### Scaling Schedulers

```bash
# Scale schedulers manually (2-4 replicas recommended)
kubectl scale deployment/airflow-scheduler --replicas=3 -n airflow
```

> **Note:** There is no HPA for schedulers because scheduling throughput is primarily DB-bound, not CPU-bound. Manual scaling to 2-4 replicas is the recommended approach.

### Prerequisites

- PVCs (`airflow-dags`, `airflow-plugins`, etc.) must support **ReadWriteMany** (RWX) access mode since scheduler, dag-processor, and worker pods mount them simultaneously.
- An `airflow-apiserver` Service must exist in k8s (referenced by `EXECUTION_API_SERVER_URL`).

---

## 🔴 Redis Sentinel (HA)

Redis is used as a transient credential vault for Airflow tasks. In production, a single Redis instance is a single point of failure — if it goes down mid-pipeline, all in-flight tasks that call `fetch_credentials()` will fail.

### Architecture

| Component | Replicas | Purpose |
|-----------|----------|---------|
| **Redis Data Nodes** | 3 (StatefulSet) | 1 master + 2 replicas. Ordinal 0 starts as master. |
| **Redis Sentinel** | 3 (Deployment) | Monitors master, triggers automatic failover (quorum: 2). |

### How Failover Works

1. Sentinels monitor the master via `PING` every second
2. If master is unreachable for 5 seconds (`down-after-milliseconds`), sentinels mark it as `SDOWN`
3. When 2/3 sentinels agree (`quorum: 2`), master is marked `ODOWN`
4. A sentinel is elected to perform failover within 10 seconds (`failover-timeout`)
5. A replica is promoted to master; other replicas reconfigure automatically
6. The Python `redis.sentinel.Sentinel` client auto-discovers the new master — no app restart needed

### Key Configuration

| Setting | Value | Purpose |
|---------|-------|---------|
| `REDIS_SENTINEL_HOSTS` | `redis-sentinel:26379` | Sentinel discovery endpoint |
| `REDIS_SENTINEL_MASTER` | `mymaster` | Sentinel master name |
| `REDIS_MAX_RETRIES` | `3` | Retry attempts for transient Redis failures |
| `REDIS_RETRY_BACKOFF_BASE` | `0.5` | Base backoff in seconds (exponential: 0.5s, 1s, 2s) |

### Error Differentiation

The `fetch_credentials()` function now distinguishes between two failure modes:

| Scenario | Exception | Airflow Behavior |
|----------|-----------|-----------------|
| Redis down (ConnectionError/TimeoutError) | `AirflowException` (after retries) | Task retries per retry config |
| Key expired / never stored | `AirflowFailException` | Task marked FAILED immediately |

### Verify Redis Sentinel

```bash
# Check Redis data pods (expect 3)
kubectl get pods -n airflow -l component=data,app=redis

# Check Sentinel pods (expect 3)
kubectl get pods -n airflow -l component=sentinel

# Query sentinel for master info
kubectl exec -n airflow deployment/redis-sentinel -- redis-cli -p 26379 SENTINEL masters

# Test failover (delete master pod, sentinel promotes replica)
kubectl delete pod -n airflow redis-0
kubectl exec -n airflow deployment/redis-sentinel -- redis-cli -p 26379 SENTINEL masters
```

### Celery Broker via Sentinel

Airflow's CeleryExecutor uses Redis as the task broker. In k8s, the broker URL uses the `sentinel://` scheme so Kombu (Celery's transport layer) asks Sentinel for the current master and auto-follows failover.

**Why not in the ConfigMap?** The broker URL contains the Redis password from `infra-secrets`. K8s ConfigMaps can't reference Secrets, so the URL is set per-Deployment using `$(REDIS_PASSWORD)` env var interpolation:

```yaml
# In airflow-worker-deployment.yaml and airflow-scheduler-deployment.yaml
env:
  - name: REDIS_PASSWORD
    valueFrom:
      secretKeyRef:
        name: infra-secrets
        key: redis_password
  - name: AIRFLOW__CELERY__BROKER_URL
    value: "sentinel://:$(REDIS_PASSWORD)@redis-sentinel:26379/0"
```

The `master_name` is set in the ConfigMap (no secret needed):
```yaml
# In airflow-config.yaml (airflow-env ConfigMap)
AIRFLOW__CELERY__BROKER_TRANSPORT_OPTIONS: '{"master_name": "mymaster"}'
```

### Docker vs K8s

| Concern | Docker (dev) | K8s (production) |
|---------|-------------|------------------|
| **Executor** | `LocalExecutor` — no Celery, no broker | `CeleryExecutor` — broker required |
| **Redis topology** | Single `redis:7-alpine` | 3 data nodes + 3 sentinels |
| **Celery broker** | N/A | `sentinel://:password@redis-sentinel:26379/0` |
| **Credential vault** | `REDIS_HOST`/`REDIS_PORT` → plain TCP | `REDIS_SENTINEL_HOSTS` → Sentinel mode |

No code changes needed between environments — Airflow reads its executor config from env vars, and the Python Redis client auto-detects Sentinel mode.

---

## 🏥 Health Checks

### Liveness Probe

Checks if the application is alive and should be restarted if unhealthy.

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8000
  initialDelaySeconds: 30
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3
```

### Readiness Probe

Checks if the application is ready to receive traffic.

```yaml
readinessProbe:
  httpGet:
    path: /ready
    port: 8000
  initialDelaySeconds: 10
  periodSeconds: 5
  timeoutSeconds: 3
  failureThreshold: 3
```

---

## 📝 Environment-Specific Configuration

### Development Environment

```yaml
# k8s/environments/dev/configmap-overrides.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: airflow-config
  namespace: airflow-dev
data:
  AIRFLOW_MAX_ACTIVE_RUNS: "100"           # Lower limits
  AIRFLOW_MAX_ACTIVE_TASKS: "30"
  AIRFLOW_RETRIES: "1"                   # Fewer retries
  AIRFLOW_RETRY_DELAY_MINUTES: "1"
  LOG_LEVEL: "DEBUG"                     # More logging
```

### Staging Environment

```yaml
# k8s/environments/staging/configmap-overrides.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: airflow-config
  namespace: airflow-staging
data:
  AIRFLOW_MAX_ACTIVE_RUNS: "100"
  AIRFLOW_MAX_ACTIVE_TASKS: "30"
  AIRFLOW_RETRIES: "3"
  LOG_LEVEL: "INFO"
```

### Production Environment

```yaml
# k8s/environments/prod/configmap-overrides.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: airflow-config
  namespace: airflow-prod
data:
  AIRFLOW_MAX_ACTIVE_RUNS: "100"          # Higher limits
  AIRFLOW_MAX_ACTIVE_TASKS: "30"
  AIRFLOW_MAX_ACTIVE_RUNS_ONDEMAND: "50"
  AIRFLOW_RETRIES: "5"                   # More retries
  AIRFLOW_EMAIL_ON_FAILURE: "True"       # Enable alerts
  AUTH_ENABLED: "True"                   # Enable auth
  LOG_LEVEL: "WARNING"                   # Less logging
```

---

## 🔧 Configuration Management Strategies

### Strategy 1: Kustomize

```bash
# Directory structure
k8s/
├── base/
│   ├── configmaps/
│   ├── deployments/
│   └── kustomization.yaml
└── overlays/
    ├── dev/
    │   └── kustomization.yaml
    ├── staging/
    │   └── kustomization.yaml
    └── prod/
        └── kustomization.yaml

# Deploy to dev
kubectl apply -k k8s/overlays/dev

# Deploy to prod
kubectl apply -k k8s/overlays/prod
```

### Strategy 2: Helm

```yaml
# values.yaml
airflow:
  config:
    maxActiveRuns: 10
    maxActiveTasks: 5
    retries: 3

# values-prod.yaml
airflow:
  config:
    maxActiveRuns: 20
    maxActiveTasks: 10
    retries: 5

# Deploy to prod
helm install airflow ./chart -f values.yaml -f values-prod.yaml
```

### Strategy 3: ArgoCD (GitOps)

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: airflow-multi-tenant
spec:
  source:
    repoURL: https://github.com/your-org/airflow-multi-tenant
    path: k8s/overlays/prod
    targetRevision: main
  destination:
    server: https://kubernetes.default.svc
    namespace: airflow-prod
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
```

---

## 🧪 Testing Configuration

### Verify ConfigMap Mounted

```bash
# Check environment variables in pod
kubectl exec -n airflow deployment/airflow-worker -- env | grep AIRFLOW_

# Expected output:
AIRFLOW_MAX_ACTIVE_RUNS=100
AIRFLOW_MAX_ACTIVE_TASKS=30
AIRFLOW_RETRIES=3
...
```

### Verify Configuration Loaded

```bash
# Check application logs
kubectl logs -n airflow deployment/airflow-worker | grep "Configuration"

# Expected output:
=== Airflow Configuration (from environment) ===
Max Active Runs (Daily): 10
Max Active Tasks: 5
...
```

### Test Configuration Changes

```bash
# Update ConfigMap
kubectl edit configmap airflow-config -n airflow

# Rollout restart to pick up changes
kubectl rollout restart deployment/airflow-worker -n airflow

# Monitor rollout
kubectl rollout status deployment/airflow-worker -n airflow
```

---

## 🚨 Troubleshooting

### Issue: Configuration Not Loading

**Symptoms:**
- Application using default values instead of ConfigMap values

**Solution:**
```bash
# 1. Verify ConfigMap exists
kubectl get configmap airflow-config -n airflow

# 2. Check ConfigMap content
kubectl describe configmap airflow-config -n airflow

# 3. Verify envFrom in deployment
kubectl get deployment airflow-worker -n airflow -o yaml | grep -A 5 envFrom

# 4. Restart pods
kubectl rollout restart deployment/airflow-worker -n airflow
```

### Issue: Secrets Not Found

**Symptoms:**
- Pod in CrashLoopBackOff
- Error: "Secret 'airflow-secrets' not found"

**Solution:**
```bash
# 1. Check secret exists
kubectl get secret airflow-secrets -n airflow

# 2. Create secret if missing
kubectl create secret generic airflow-secrets \
  --from-literal=AIRFLOW_PASSWORD='changeme' \
  --namespace=airflow

# 3. Verify secret mounted
kubectl describe pod <pod-name> -n airflow | grep -A 10 "Secret"
```

### Issue: KEDA Not Scaling

**Symptoms:**
- Workers not scaling based on task queue

**Solution:**
```bash
# 1. Check KEDA installed
kubectl get pods -n keda

# 2. Check ScaledObject
kubectl get scaledobject -n airflow

# 3. Check KEDA logs
kubectl logs -n keda deployment/keda-operator

# 4. Verify trigger query
kubectl describe scaledobject airflow-worker-scaler -n airflow
```

---

## 📚 Best Practices

### ✅ DO

1. **Use ConfigMaps for non-sensitive data**
   - Concurrency limits
   - Retry policies
   - Timeouts

2. **Use Secrets for sensitive data**
   - Passwords
   - API keys
   - JWT secrets

3. **Set resource limits**
   - Prevents resource exhaustion
   - Enables proper autoscaling

4. **Use health checks**
   - Liveness probes detect crashes
   - Readiness probes prevent bad traffic routing

5. **Enable autoscaling**
   - KEDA for workers (task queue depth)
   - HPA for control plane (CPU/memory)

6. **Use proper secret management**
   - External Secrets Operator
   - HashiCorp Vault
   - Cloud provider secret managers

7. **Implement monitoring**
   - Prometheus metrics
   - Grafana dashboards
   - Alert on failures

### ❌ DON'T

1. **Don't hardcode configuration in code**
   - Always use environment variables
   - Provide fallback defaults

2. **Don't store secrets in ConfigMaps**
   - Use Kubernetes Secrets
   - Use external secret management

3. **Don't commit secrets to Git**
   - Use .gitignore
   - Use sealed secrets or encryption

4. **Don't skip resource limits**
   - Can lead to node exhaustion
   - Prevents proper scheduling

5. **Don't disable health checks**
   - Kubernetes can't detect failures
   - Bad pods receive traffic

---

## ⚠️ Schema Change Checklist {#schema-change-checklist}

When modifying `packages/shared_models/shared_models/tables.py`, some downstream files contain **raw SQL** that references table/column names directly. These must be updated manually — they are not covered by Alembic migrations.

| File | Raw SQL Location | Tables/Columns Referenced |
|------|-----------------|--------------------------|
| `k8s/deployments/airflow-worker-deployment.yaml` | KEDA MySQL trigger | `integrations.usr_sch_status`, `integrations.schedule_type`, `integrations.utc_next_run` |
| `k8s/deployments/airflow-worker-deployment.yaml` | KEDA PostgreSQL trigger | `task_instance.state` (Airflow internal — rarely changes) |

### Steps After Schema Change

1. **Generate Alembic migration** as usual (`alembic revision --autogenerate`)
2. **Search for raw SQL references** to any renamed/removed columns:
   ```bash
   grep -r 'integrations\|task_instance' k8s/deployments/ --include='*.yaml'
   ```
3. **Update KEDA queries** in `airflow-worker-deployment.yaml` if affected
4. **Apply in order**: ConfigMaps → Alembic migration (init container) → KEDA ScaledObject

---

## 📖 Related Documentation

- [Airflow Best Practices](../airflow/docs/AIRFLOW_BEST_PRACTICES.md)
- [Worker Efficiency](../docs/WORKER_EFFICIENCY_IMPROVEMENTS.md)
- [DST Developer Guide](../airflow/docs/DST_DEVELOPER_GUIDE.md)
- [Backfill Strategy](../airflow/docs/BACKFILL_STRATEGY.md)

---

## 🔗 External Resources

- [Kubernetes ConfigMaps](https://kubernetes.io/docs/concepts/configuration/configmap/)
- [Kubernetes Secrets](https://kubernetes.io/docs/concepts/configuration/secret/)
- [KEDA Documentation](https://keda.sh/docs/)
- [External Secrets Operator](https://external-secrets.io/)
- [Sealed Secrets](https://github.com/bitnami-labs/sealed-secrets)

---

**Last Updated:** 2026-02-03
**Status:** ✅ Production Ready
**Kubernetes Version:** 1.19+
