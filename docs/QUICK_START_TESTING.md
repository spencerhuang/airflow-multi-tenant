# Quick Start Testing Guide

## ⚡ Fast Track (TL;DR)

```bash
# 1. Start Docker Desktop/OrbStack
# 2. Run this:
./run_all_tests.sh
```

That's it! 🎉

---

## 📝 What Got Fixed

**Problem:** The script was trying to install `apache-airflow-providers-mongo==3.5.1` which doesn't exist.

**Solution:**
1. ✅ Fixed version in `requirements.txt` (3.5.1 → 5.3.2)
2. ✅ Created `requirements-test.txt` with only test dependencies (no heavy Airflow)
3. ✅ Updated `run_all_tests.sh` to use lightweight test dependencies

---

## 🚀 Running Tests

### Option 1: All Tests (Recommended)

```bash
# Make sure Docker is running!
./run_all_tests.sh
```

This will:
- ✓ Check Docker is running
- ✓ Start all services (MySQL, Kafka, MongoDB, etc.)
- ✓ Install test dependencies automatically
- ✓ Run 73 unit tests (94% coverage)
- ✓ Run integration tests with live database
- ✓ Run CDC Kafka tests
- ✓ Generate HTML coverage reports

### Option 2: Unit Tests Only (No Docker)

```bash
source venv/bin/activate
pytest connectors/tests/ control_plane/tests/test_integration_service.py -v
```

### Option 3: Step by Step

```bash
# 1. Install test dependencies
uv pip install -r requirements-test.txt

# 2. Start Docker services
docker-compose up -d

# 3. Wait for services (60 seconds)
sleep 60

# 4. Run unit tests
pytest connectors/tests/ control_plane/tests/test_integration_service.py -v

# 5. Run integration tests
pytest control_plane/tests/test_integration_api_live.py -v -s

# 6. Run CDC Kafka tests
pytest control_plane/tests/test_cdc_kafka.py -v -s
```

---

## 📦 Test Requirements

### Lightweight (requirements-test.txt)
- **What:** Only test dependencies (no Airflow)
- **Size:** ~200MB
- **Install time:** 1-2 minutes
- **Use for:** Running tests locally

```bash
uv pip install -r requirements-test.txt
```

### Full (requirements.txt + requirements-dev.txt)
- **What:** All dependencies including Airflow
- **Size:** ~2GB
- **Install time:** 10-15 minutes
- **Use for:** Full development environment

```bash
uv pip install -r requirements.txt
uv pip install -r requirements-dev.txt
```

---

## 🐳 Docker Services Status

Check if services are running:

```bash
# All services
docker-compose ps

# Control Plane API
curl http://localhost:8000/api/v1/health

# Airflow
curl http://localhost:8080/health

# MySQL
docker exec control-plane-mysql mysqladmin ping -h localhost -proot

# Kafka
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

---

## 📊 Test Coverage

After running tests, view the coverage report:

```bash
# Open in browser
open htmlcov/all/index.html

# Or terminal output shows coverage automatically
```

Current coverage: **94%**

---

## 🔧 Troubleshooting

### "Cannot connect to Docker daemon"

```bash
# Solution: Start Docker Desktop or OrbStack
# Then run the script again
./run_all_tests.sh
```

### "ModuleNotFoundError: No module named 'pytest'"

```bash
# Install test dependencies
uv pip install -r requirements-test.txt
```

### Tests timeout or fail

```bash
# Services might not be ready, wait longer
docker-compose ps  # Check all services are "Up"
sleep 30           # Wait more
pytest <test_file> # Try again
```

### Clean slate

```bash
# Stop and remove everything
docker-compose down -v
docker volume prune -f

# Start fresh
./run_all_tests.sh
```

---

## 📁 Test Files

| File | Tests | Requires Docker |
|------|-------|-----------------|
| `test_s3_client.py` | 10 | No |
| `test_s3_reader.py` | 15 | No |
| `test_mongo_client.py` | 17 | No |
| `test_azure_blob_client.py` | 15 | No |
| `test_integration_service.py` | 16 | No |
| `test_integration_api_live.py` | 7 | **Yes** |
| `test_cdc_kafka.py` | 10+ | **Yes** |

**Total:** 90+ tests

---

## 🎯 Next Steps

1. **Run tests:** `./run_all_tests.sh`
2. **Check coverage:** `open htmlcov/all/index.html`
3. **Read docs:** See [TESTING.md](TESTING.md) for details
4. **Add tests:** Follow examples in test files

---

## 💡 Tips

- **Fast feedback:** Run unit tests first (no Docker needed)
- **Integration tests:** Only run when needed (slower)
- **CI/CD:** Use `run_all_tests.sh` in your pipeline
- **Coverage:** Aim for >90% on new code

---

**Need help?** See [TESTING.md](TESTING.md) for comprehensive guide.
