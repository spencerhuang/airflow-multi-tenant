

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
│   ├── airflow-scheduler-deployment.yaml
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
# Deploy Control Plane
kubectl apply -f k8s/deployments/control-plane-deployment.yaml

# Deploy Airflow Workers
kubectl apply -f k8s/deployments/airflow-worker-deployment.yaml

# Deploy Airflow Scheduler (similar pattern)
# kubectl apply -f k8s/deployments/airflow-scheduler-deployment.yaml
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

KEDA (Kubernetes Event-Driven Autoscaling) scales workers based on task queue depth.

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
    - type: postgresql
      metadata:
        query: "SELECT COUNT(*) FROM task_instance WHERE state IN ('queued', 'scheduled')"
        targetQueryValue: "10"  # 10 tasks per worker
```

**Benefits:**
- Scales to 0 workers when idle (cost savings)
- Scales up to 50 workers during backfill (handles load)
- Automatic scaling based on actual workload

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
