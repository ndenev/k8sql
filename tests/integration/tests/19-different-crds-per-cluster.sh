#!/usr/bin/env bash
# Test different CRDs across clusters
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== Different CRDs Per Cluster Tests ==="

# testresources CRD exists in both clusters
assert_table_contains "testresources in cluster 1 SHOW TABLES" "k3d-k8sql-test-1" \
    "SHOW TABLES" "testresources"

assert_table_contains "testresources in cluster 2 SHOW TABLES" "k3d-k8sql-test-2" \
    "SHOW TABLES" "testresources"

# configs CRD exists only in cluster 2
assert_table_contains "configs in cluster 2 SHOW TABLES" "k3d-k8sql-test-2" \
    "SHOW TABLES" "configs"

assert_not_contains "configs NOT in cluster 1 SHOW TABLES" "k3d-k8sql-test-1" \
    "SHOW TABLES" "configs"

# Query testresources works in both clusters
assert_row_count "Query testresources in cluster 1" "k3d-k8sql-test-1" \
    "SELECT name FROM testresources WHERE namespace = 'default'" 1

assert_row_count "Query testresources in cluster 2" "k3d-k8sql-test-2" \
    "SELECT name FROM testresources WHERE namespace = 'default'" 1

# Query configs works in cluster 2
assert_row_count "Query configs in cluster 2" "k3d-k8sql-test-2" \
    "SELECT name FROM configs WHERE namespace = 'default'" 1

assert_contains "Config spec->key field" "k3d-k8sql-test-2" \
    "SELECT spec FROM configs WHERE name = 'test-config'" "database.host"

assert_contains "Config spec->value field" "k3d-k8sql-test-2" \
    "SELECT spec FROM configs WHERE name = 'test-config'" "postgres.example.com"

# Query configs using short name alias 'cfg'
assert_contains "Query configs using short name 'cfg'" "k3d-k8sql-test-2" \
    "SELECT name FROM cfg WHERE name = 'test-config'" "test-config"

# Query configs fails in cluster 1 (table doesn't exist)
assert_error "Query configs in cluster 1 fails" "k3d-k8sql-test-1" \
    "SELECT name FROM configs" "table.*not found"

# Multi-cluster wildcard query for testresources (exists in both)
assert_min_row_count "testresources across both clusters" "k3d-k8sql-test-1" \
    "SELECT name, _cluster FROM testresources WHERE _cluster = '*'" 4

# Multi-cluster wildcard query for configs (exists only in cluster 2)
# Must query from cluster 2's context (which knows about configs table)
# Cluster 1 will return 404 (handled gracefully), cluster 2 returns data
assert_row_count "configs wildcard query from cluster 2 context" "k3d-k8sql-test-2" \
    "SELECT name, _cluster FROM configs WHERE _cluster = '*'" 2

assert_contains "configs wildcard shows only cluster 2 results" "k3d-k8sql-test-2" \
    "SELECT name, _cluster FROM configs WHERE _cluster = '*'" "k3d-k8sql-test-2"

# Verify cluster 2 has both CRDs available
assert_row_count "Cluster 2 has both testresources" "k3d-k8sql-test-2" \
    "SELECT name FROM testresources" 2

assert_row_count "Cluster 2 has both configs" "k3d-k8sql-test-2" \
    "SELECT name FROM configs" 2

print_summary
