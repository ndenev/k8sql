#!/usr/bin/env bash
# Error handling tests for k8sql
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== Error Handling Tests ==="

# Read-only enforcement - CREATE TABLE
assert_error "CREATE TABLE blocked" "k3d-k8sql-test-1" \
    "CREATE TABLE test (id INT)" "read-only"

# Read-only enforcement - INSERT
assert_error "INSERT blocked" "k3d-k8sql-test-1" \
    "INSERT INTO pods VALUES (1)" "read-only"

# Read-only enforcement - UPDATE
assert_error "UPDATE blocked" "k3d-k8sql-test-1" \
    "UPDATE pods SET name = 'x'" "read-only"

# Read-only enforcement - DELETE
assert_error "DELETE blocked" "k3d-k8sql-test-1" \
    "DELETE FROM pods" "read-only"

# Read-only enforcement - DROP TABLE
assert_error "DROP TABLE blocked" "k3d-k8sql-test-1" \
    "DROP TABLE pods" "read-only"

# Read-only enforcement - ALTER TABLE
assert_error "ALTER TABLE blocked" "k3d-k8sql-test-1" \
    "ALTER TABLE pods ADD COLUMN new_col VARCHAR" "read-only"

# Read-only enforcement - TRUNCATE
assert_error "TRUNCATE blocked" "k3d-k8sql-test-1" \
    "TRUNCATE TABLE pods" "read-only"

# Invalid table name
assert_error "Invalid table error" "k3d-k8sql-test-1" \
    "SELECT * FROM nonexistent_table_xyz" "table"

# Invalid column name
assert_error "Invalid column error" "k3d-k8sql-test-1" \
    "SELECT nonexistent_column_xyz FROM pods" "column"

# SQL syntax error
assert_error "SQL syntax error" "k3d-k8sql-test-1" \
    "SELECTT * FROM pods" "error"

# Valid queries still work after errors
assert_success "Recovery after error" "k3d-k8sql-test-1" \
    "SELECT name FROM pods LIMIT 1"

print_summary
