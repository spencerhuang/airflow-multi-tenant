# Foreign Key Constraint Fix

## ✅ Issue Resolved

**Problem:** Integration tests were failing with foreign key constraint violations when trying to clean up test data.

**Error:**
```
sqlalchemy.exc.IntegrityError: (pymysql.err.IntegrityError)
(1451, 'Cannot delete or update a parent row: a foreign key constraint fails
(`control_plane`.`workspaces`, CONSTRAINT `workspaces_ibfk_1`
FOREIGN KEY (`customer_guid`) REFERENCES `customers` (`customer_guid`))')
```

---

## 🔍 Root Cause

The test cleanup was trying to delete parent records before child records:

```python
# ❌ WRONG ORDER (caused foreign key errors)
db.query(Customer).filter(...).delete()  # Parent
# But Workspaces still reference these Customers!
```

This violated foreign key constraints because child records (workspaces) still referenced parent records (customers).

---

## ✅ Solution Applied

**File:** `control_plane/tests/test_integration_api_live.py`

Fixed the cleanup order to delete children first, then parents:

```python
# ✅ CORRECT ORDER (respects foreign keys)
from control_plane.app.models.integration import Integration

# Delete in correct order: children first, parents last
db.query(Integration).filter(Integration.workspace_id.like("test-%")).delete(synchronize_session=False)
db.query(Auth).filter(Auth.workspace_id.like("test-%")).delete(synchronize_session=False)
db.query(Workspace).filter(Workspace.workspace_id.like("test-%")).delete(synchronize_session=False)
db.query(Customer).filter(Customer.customer_guid.like("test-%")).delete(synchronize_session=False)
db.commit()
```

---

## 📊 Deletion Order

The correct order based on foreign key relationships:

```
1. Integration
   ├─ references: workspace_id (Workspace)
   └─ references: auth_id (Auth)

2. Auth
   └─ references: workspace_id (Workspace)

3. Workspace
   └─ references: customer_guid (Customer)

4. Customer
   └─ (parent table, no foreign keys)
```

**Rule:** Always delete child records before parent records!

---

## 🔧 Additional Improvements

### 1. Added `synchronize_session=False`

```python
.delete(synchronize_session=False)
```

This improves performance and avoids session synchronization issues during bulk deletes.

### 2. Added Error Handling in Cleanup

```python
finally:
    try:
        # Delete operations...
        db.commit()
    except Exception as e:
        print(f"Cleanup error (non-fatal): {e}")
        db.rollback()
    finally:
        db.close()
        engine.dispose()
```

This ensures cleanup errors don't break the test suite.

---

## 🧪 Testing

The fix is applied in both places:

### 1. Setup (before tests)
```python
# Clean up any existing test data from previous runs
db.query(Integration).filter(...).delete(synchronize_session=False)
db.query(Auth).filter(...).delete(synchronize_session=False)
db.query(Workspace).filter(...).delete(synchronize_session=False)
db.query(Customer).filter(...).delete(synchronize_session=False)
db.commit()
```

### 2. Teardown (after tests)
```python
finally:
    try:
        # Same order for cleanup
        db.query(Integration).filter(...).delete(synchronize_session=False)
        db.query(Auth).filter(...).delete(synchronize_session=False)
        db.query(Workspace).filter(...).delete(synchronize_session=False)
        db.query(Customer).filter(...).delete(synchronize_session=False)
        db.commit()
    except Exception as e:
        print(f"Cleanup error (non-fatal): {e}")
        db.rollback()
```

---

## 📈 Impact

**Before:** All integration tests failing with foreign key errors

**After:** Integration tests ready to run (require Docker)

---

## 🚀 Running the Tests

Now you can run the integration tests:

```bash
# Start Docker services
docker-compose up -d

# Wait for services to be ready
sleep 60

# Run integration tests
pytest control_plane/tests/test_integration_api_live.py -v -s
```

Or use the automated script:

```bash
./run_all_tests.sh
```

---

## 🎓 Lessons Learned

### Foreign Key Best Practices

1. **Always delete in order:**
   - Children first (tables with foreign keys)
   - Parents last (tables referenced by others)

2. **Understand your schema:**
   ```
   Customer (parent)
     ↓ referenced by
   Workspace (child)
     ↓ referenced by
   Auth, Integration (grandchildren)
   ```

3. **Use transactions:**
   - Group deletes in a single transaction
   - Rollback on error to maintain consistency

4. **Consider CASCADE:**
   - Alternative: Use `ON DELETE CASCADE` in schema
   - Pro: Automatic cleanup
   - Con: Less control, potential for accidents

---

## 📁 Files Modified

| File | Change |
|------|--------|
| `control_plane/tests/test_integration_api_live.py` | Fixed deletion order |

---

## ✅ Verification

Cleanup order verification:

```
1. Integration → references workspace, auth
2. Auth → references workspace
3. Workspace → references customer
4. Customer → parent table

✅ CORRECT - children deleted before parents
```

---

## 🔗 Related Documentation

- [FIXES_APPLIED.md](FIXES_APPLIED.md) - All fixes summary
- [TESTING.md](TESTING.md) - Testing guide
- [QUICK_START_TESTING.md](QUICK_START_TESTING.md) - Quick start

---

**Status:** ✅ Fixed and ready for testing!
