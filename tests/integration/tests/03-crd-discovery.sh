#!/usr/bin/env bash
# CRD discovery tests for k8sql
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== CRD Discovery Tests ==="

# CRD appears in SHOW TABLES
assert_table_contains "CRD in SHOW TABLES" "k3d-k8sql-test-1" \
    "SHOW TABLES" "testresources"

# Query CRD instances - we created 2 (one in default, one in test-ns)
assert_row_count "Query CRD instances in default" "k3d-k8sql-test-1" \
    "SELECT name FROM testresources WHERE namespace = 'default'" 1

assert_row_count "Query all CRD instances" "k3d-k8sql-test-1" \
    "SELECT name FROM testresources" 2

# CRD instance names
assert_contains "CRD instance test-cr exists" "k3d-k8sql-test-1" \
    "SELECT name FROM testresources" "test-cr"

assert_contains "CRD instance test-cr-2 exists" "k3d-k8sql-test-1" \
    "SELECT name FROM testresources" "test-cr-2"

# CRD spec field access
assert_contains "CRD spec->message field" "k3d-k8sql-test-1" \
    "SELECT spec FROM testresources WHERE name = 'test-cr'" "Hello from CRD"

# CRD spec->count field
assert_contains "CRD spec->count field" "k3d-k8sql-test-1" \
    "SELECT spec FROM testresources WHERE name = 'test-cr'" "42"

# CRD labels
assert_contains "CRD labels" "k3d-k8sql-test-1" \
    "SELECT labels FROM testresources WHERE name = 'test-cr'" "test-crd"

# Filter CRD by namespace
assert_row_count "Filter CRD by namespace" "k3d-k8sql-test-1" \
    "SELECT name FROM testresources WHERE namespace = 'test-ns'" 1

# CRD short name alias (tr) - verify it appears in table list
assert_table_contains "CRD short name alias in SHOW TABLES" "k3d-k8sql-test-1" \
    "SHOW TABLES" "testresources"

# CRD across clusters with wildcard
assert_min_row_count "CRD across clusters" "k3d-k8sql-test-1" \
    "SELECT name, _cluster FROM testresources WHERE _cluster = '*'" 4

print_summary
