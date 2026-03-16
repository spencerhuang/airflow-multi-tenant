# Docker Build Fix - SQLAlchemy Conflict Resolution

## 🔴 The Problem

When running `./run_all_tests.sh`, the Docker build failed with:

```
ERROR: Cannot install -r requirements.txt (line 2) and sqlalchemy==2.0.25
because these package versions have conflicting dependencies.

The conflict is caused by:
    The user requested sqlalchemy==2.0.25
    apache-airflow 2.8.1 depends on sqlalchemy<2.0 and >=1.4.28
```

**Root cause:** Apache Airflow 2.8.1 requires SQLAlchemy <2.0, but requirements.txt had SQLAlchemy 2.0.25.

---

## ✅ The Solution

Created **separate requirements files** for different purposes:

### 1. requirements-control-plane.txt (NEW)
- **For:** Control Plane Docker container
- **Contains:** Only what the control plane needs (no Airflow!)
- **SQLAlchemy:** 2.0.25 (latest version)
- **Size:** ~50 packages
- **Build time:** ~20 seconds

### 2. requirements-test.txt (Already created)
- **For:** Running tests locally
- **Contains:** Test dependencies only (no Airflow!)
- **SQLAlchemy:** 2.0.46 (latest version)
- **Size:** ~40 packages
- **Install time:** 1-2 minutes

### 3. requirements.txt (UPDATED)
- **For:** Full Airflow installation
- **Contains:** All dependencies including Airflow
- **SQLAlchemy:** 1.4.53 (Airflow-compatible)
- **Size:** ~200+ packages
- **Install time:** 10-15 minutes

---

## 📝 Changes Made

### File: docker/Dockerfile.control-plane

**Before:**
```dockerfile
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
```

**After:**
```dockerfile
COPY requirements-control-plane.txt .
RUN pip install --no-cache-dir -r requirements-control-plane.txt
```

### File: requirements.txt

**Before:**
```
sqlalchemy==2.0.25
```

**After:**
```
# NOTE: Airflow 2.8.1 requires SQLAlchemy <2.0
sqlalchemy==1.4.53
```

### File: requirements-control-plane.txt (NEW)

Created with only control plane dependencies:
- FastAPI, Uvicorn, Pydantic
- SQLAlchemy 2.0.25, PyMySQL, Alembic
- Boto3, Azure SDK, PyMongo
- Kafka-python, APScheduler
- Authentication libs
- **No Airflow!**

---

## ✅ Verification

Control plane container built successfully:

```bash
docker-compose build control-plane
# ✓ Successfully installed 40 packages
# ✓ No conflicts
# ✓ Build time: ~20 seconds
```

---

## 🚀 Ready to Run

Now you can run the full test suite:

```bash
# Start Docker services
docker-compose up -d

# Or use the test script (does this automatically)
./run_all_tests.sh
```

---

## 📊 Requirements Files Summary

| File | Purpose | Airflow? | SQLAlchemy | Packages |
|------|---------|----------|------------|----------|
| `requirements.txt` | Full dev environment | ✅ Yes | 1.4.53 | ~200 |
| `requirements-control-plane.txt` | Control plane container | ❌ No | 2.0.25 | ~50 |
| `requirements-test.txt` | Local testing | ❌ No | 2.0.46 | ~40 |
| `requirements-dev.txt` | Dev tools (linting, etc) | ❌ No | - | ~15 |

---

## 🎯 When to Use Each File

### Local Development

```bash
# Full environment (with Airflow)
uv pip install -r requirements.txt
uv pip install -r requirements-dev.txt

# Testing only (no Airflow, faster)
uv pip install -r requirements-test.txt

# Control plane only (no Airflow)
uv pip install -r requirements-control-plane.txt
```

### Docker

- **Control Plane:** Uses `requirements-control-plane.txt` automatically
- **Airflow:** Uses Airflow Docker image (pre-built)
- **All services:** Just run `docker-compose up -d`

---

## 🔍 Why This Approach?

### Problem with Single requirements.txt

```
Control Plane needs SQLAlchemy 2.0
      ↓
Install Airflow for completeness
      ↓
Airflow requires SQLAlchemy <2.0
      ↓
CONFLICT! 💥
```

### Solution: Separate Requirements

```
Control Plane → requirements-control-plane.txt → SQLAlchemy 2.0 ✅
Airflow      → Docker image                   → SQLAlchemy 1.4 ✅
Testing      → requirements-test.txt          → SQLAlchemy 2.0 ✅
```

No conflicts! Each service gets exactly what it needs.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Docker Compose                                         │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────────┐      ┌──────────────────┐       │
│  │ Control Plane    │      │ Airflow          │       │
│  │                  │      │                  │       │
│  │ requirements-    │      │ Apache Airflow   │       │
│  │ control-plane.txt│      │ Docker Image     │       │
│  │                  │      │                  │       │
│  │ SQLAlchemy 2.0   │      │ SQLAlchemy 1.4   │       │
│  └──────────────────┘      └──────────────────┘       │
│          ✅                         ✅                  │
│    No Conflict!             No Conflict!               │
└─────────────────────────────────────────────────────────┘
```

---

## 📚 Related Files

- [QUICK_START_TESTING.md](QUICK_START_TESTING.md) - How to run tests
- [TESTING.md](../TESTING.md) - Comprehensive testing guide
- [run_all_tests.sh](run_all_tests.sh) - Automated test script
- [docker-compose.yml](docker-compose.yml) - Service definitions

---

## ✨ Next Steps

1. **Start services:**
   ```bash
   docker-compose up -d
   ```

2. **Run tests:**
   ```bash
   ./run_all_tests.sh
   ```

3. **Check everything works:**
   ```bash
   # Control Plane API
   curl http://localhost:8000/api/v1/health

   # Airflow UI
   curl http://localhost:8080/health
   ```

**All set! 🎉**
