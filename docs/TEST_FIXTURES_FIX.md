# Test Fixtures Duplicate Key Fix

## ✅ Issue Resolved

**Problem:** Integration tests were failing with duplicate primary key errors for access points and workflows.

**Error:**
```
pymysql.err.IntegrityError: (1062, "Duplicate entry '1' for key 'access_points.PRIMARY'")
```

---

## 🔍 Root Cause

The test setup was trying to insert fixtures (AccessPoints and Workflows) with fixed IDs without checking if they already existed from previous test runs:

```python
# ❌ PROBLEM: Always tries to insert
workflow = Workflow(workflow_id=1, workflow_type="S3ToMongo")
db.add(workflow)

s3_ap = AccessPoint(access_pt_id=1, ap_type="S3")
mongo_ap = AccessPoint(access_pt_id=2, ap_type="MongoDB")
db.add_all([s3_ap, mongo_ap])
```

If these records already existed in the database (from a previous test run), this would cause a duplicate key error.

---

## ✅ Solution Applied

**File:** `control_plane/tests/test_integration_api_live.py`

Implemented the **"get-or-create"** pattern:

```python
# ✅ SOLUTION: Get existing or create new

# Create or get workflow (may already exist)
workflow = db.query(Workflow).filter(Workflow.workflow_id == 1).first()
if not workflow:
    workflow = Workflow(workflow_id=1, workflow_type="S3ToMongo")
    db.add(workflow)

# Create or get access points (may already exist)
s3_ap = db.query(AccessPoint).filter(AccessPoint.access_pt_id == 1).first()
if not s3_ap:
    s3_ap = AccessPoint(access_pt_id=1, ap_type="S3")
    db.add(s3_ap)

mongo_ap = db.query(AccessPoint).filter(AccessPoint.access_pt_id == 2).first()
if not mongo_ap:
    mongo_ap = AccessPoint(access_pt_id=2, ap_type="MongoDB")
    db.add(mongo_ap)

db.commit()
```

---

## 🎯 Why This Works

### Before (Problematic)
```
Test Run 1:
  → Insert Workflow(id=1) ✓
  → Insert AccessPoint(id=1) ✓
  → Insert AccessPoint(id=2) ✓

Test Run 2:
  → Insert Workflow(id=1) ❌ DUPLICATE KEY!
  → Insert AccessPoint(id=1) ❌ DUPLICATE KEY!
```

### After (Fixed)
```
Test Run 1:
  → Check if Workflow(id=1) exists → No → Create ✓
  → Check if AccessPoint(id=1) exists → No → Create ✓
  → Check if AccessPoint(id=2) exists → No → Create ✓

Test Run 2:
  → Check if Workflow(id=1) exists → Yes → Use existing ✓
  → Check if AccessPoint(id=1) exists → Yes → Use existing ✓
  → Check if AccessPoint(id=2) exists → Yes → Use existing ✓
```

---

## 📊 Fixture Types

The test fixtures are divided into two categories:

### 1. Test-Specific Data (Cleaned Up)
These are created fresh for each test and cleaned up afterward:
- ✅ **Customer** (`test-cust-001`)
- ✅ **Workspace** (`test-ws-001`)
- ✅ **Auth** (test auth record)
- ✅ **Integration** (created by tests)

**Cleanup:** Deleted after each test run

### 2. Shared Reference Data (Persistent)
These are reference data that can be shared across tests:
- ✅ **Workflow** (id=1, type="S3ToMongo")
- ✅ **AccessPoint** (id=1, type="S3")
- ✅ **AccessPoint** (id=2, type="MongoDB")

**Pattern:** Get-or-create (not deleted)

---

## 🔧 Implementation Details

### Get-or-Create Pattern

```python
# Step 1: Try to get existing
record = db.query(Model).filter(Model.id == value).first()

# Step 2: Create only if not found
if not record:
    record = Model(id=value, ...)
    db.add(record)
```

### Why Not Clean Up Reference Data?

Reference data (workflows, access points) are:
- **Not test-specific** - generic reference data
- **Can be reused** - same across all tests
- **Rarely change** - static configuration
- **Safe to share** - no test contamination

---

## 🧪 Test Isolation

### Test-Specific Data Pattern

```python
# Setup
customer = Customer(customer_guid="test-cust-001", ...)
db.add(customer)
db.commit()

# ... run tests ...

# Cleanup (respecting foreign keys)
db.query(Integration).filter(...).delete()  # Children first
db.query(Auth).filter(...).delete()
db.query(Workspace).filter(...).delete()
db.query(Customer).filter(...).delete()     # Parent last
db.commit()
```

This ensures:
- ✅ Each test starts with clean slate for test data
- ✅ No contamination between test runs
- ✅ Reference data is preserved
- ✅ No duplicate key errors

---

## 🎓 Best Practices

### 1. Separate Test Data from Reference Data

**Test Data:**
- Unique per test run
- Prefixed (e.g., "test-*")
- Cleaned up after use

**Reference Data:**
- Shared across tests
- Fixed IDs
- Get-or-create pattern

### 2. Use Meaningful Prefixes

```python
# Good: Easy to identify and clean up
customer_guid="test-cust-001"
workspace_id="test-ws-001"

# Bad: Could conflict with real data
customer_guid="cust-001"
workspace_id="ws-001"
```

### 3. Foreign Key Aware Cleanup

```python
# Always delete in order:
# 1. Children (reference others)
# 2. Parents (referenced by others)
db.query(Child).filter(...).delete()
db.query(Parent).filter(...).delete()
```

---

## ✅ Verification

The fix ensures:

- ✅ **First run:** Creates all fixtures
- ✅ **Subsequent runs:** Reuses reference data, recreates test data
- ✅ **No duplicate keys:** Get-or-create prevents errors
- ✅ **Proper cleanup:** Test data removed, reference data preserved
- ✅ **Test isolation:** Each test gets fresh test data

---

## 📁 Files Modified

| File | Change |
|------|--------|
| `control_plane/tests/test_integration_api_live.py` | Added get-or-create for fixtures |

---

## 🚀 Running Tests

Now the tests can run multiple times without errors:

```bash
# First run
pytest control_plane/tests/test_integration_api_live.py -v
# ✓ Creates fixtures

# Second run (no database reset)
pytest control_plane/tests/test_integration_api_live.py -v
# ✓ Reuses fixtures, no duplicate key errors!

# Or use the automated script
./run_all_tests.sh
```

---

## 🎯 Summary

| Issue | Solution | Status |
|-------|----------|--------|
| Duplicate workflow ID | Get-or-create pattern | ✅ Fixed |
| Duplicate access point IDs | Get-or-create pattern | ✅ Fixed |
| Test isolation | Separate test data cleanup | ✅ Fixed |

---

## 🔗 Related Documentation

- [FOREIGN_KEY_FIX.md](FOREIGN_KEY_FIX.md) - Foreign key constraint fix
- [FIXES_APPLIED.md](FIXES_APPLIED.md) - All fixes summary
- [TESTING.md](TESTING.md) - Testing guide

---

**Status:** ✅ Fixed and ready for testing!
