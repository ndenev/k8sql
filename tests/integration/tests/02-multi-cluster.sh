#!/usr/bin/env bash
# Multi-cluster query tests for k8sql
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== Multi-Cluster Tests ==="

# Query from cluster 1 explicitly
assert_success "Query cluster 1 explicitly" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE _cluster = 'k3d-k8sql-test-1'"

# Query from cluster 2
assert_success "Query cluster 2" "k3d-k8sql-test-2" \
    "SELECT name FROM namespaces WHERE _cluster = 'k3d-k8sql-test-2'"

# Cluster 2 has the extra namespace
assert_contains "Cluster 2 has cluster2-only namespace" "k3d-k8sql-test-2" \
    "SELECT name FROM namespaces" "cluster2-only"

# Query all clusters with wildcard (from cluster 1 context)
assert_success "Wildcard cluster query" "k3d-k8sql-test-1" \
    "SELECT DISTINCT _cluster FROM namespaces WHERE _cluster = '*'"

# Wildcard should return results from both clusters
assert_contains "Wildcard returns cluster 1" "k3d-k8sql-test-1" \
    "SELECT DISTINCT _cluster FROM namespaces WHERE _cluster = '*'" "k3d-k8sql-test-1"

assert_contains "Wildcard returns cluster 2" "k3d-k8sql-test-1" \
    "SELECT DISTINCT _cluster FROM namespaces WHERE _cluster = '*'" "k3d-k8sql-test-2"

# Cluster IN list - query specific clusters (returns results from current + listed clusters)
assert_success "Cluster IN list query succeeds" "k3d-k8sql-test-1" \
    "SELECT name, _cluster FROM pods WHERE _cluster IN ('k3d-k8sql-test-1', 'k3d-k8sql-test-2') AND namespace = 'default'"

# IN list should return current cluster's pods
assert_contains "IN list returns current cluster pods" "k3d-k8sql-test-1" \
    "SELECT name, _cluster FROM pods WHERE _cluster IN ('k3d-k8sql-test-1', 'k3d-k8sql-test-2') AND namespace = 'default'" \
    "k3d-k8sql-test-1"

# IN list should also return cluster 2's pods
assert_contains "IN list returns cluster 2 pods" "k3d-k8sql-test-1" \
    "SELECT name, _cluster FROM pods WHERE _cluster IN ('k3d-k8sql-test-1', 'k3d-k8sql-test-2') AND namespace = 'default'" \
    "k3d-k8sql-test-2"

# _cluster column is present in results
assert_contains "_cluster column in output" "k3d-k8sql-test-1" \
    "SELECT _cluster, name FROM pods WHERE namespace = 'default' LIMIT 1" "k3d-k8sql-test-1"

# NOT IN tests - verify exclusion works correctly
# NOT IN should query all clusters EXCEPT the excluded ones
assert_contains "NOT IN returns cluster 1" "k3d-k8sql-test-1" \
    "SELECT DISTINCT _cluster FROM pods WHERE _cluster NOT IN ('k3d-k8sql-test-2') AND namespace = 'default'" \
    "k3d-k8sql-test-1"

assert_not_contains "NOT IN excludes cluster 2" "k3d-k8sql-test-1" \
    "SELECT DISTINCT _cluster FROM pods WHERE _cluster NOT IN ('k3d-k8sql-test-2') AND namespace = 'default'" \
    "k3d-k8sql-test-2"

echo ""
echo "=== Multi-Cluster LIMIT Tests ==="

# LIMIT across multiple clusters should return exactly the requested number of rows
# This tests that DataFusion's LimitExec correctly limits the final result
# even though each partition (cluster) may fetch up to LIMIT rows
assert_row_count "Multi-cluster LIMIT 3" "k3d-k8sql-test-1" \
    "SELECT name, _cluster FROM pods WHERE _cluster = '*' AND namespace = 'default' LIMIT 3" 3

assert_row_count "Multi-cluster LIMIT 5" "k3d-k8sql-test-1" \
    "SELECT name, _cluster FROM pods WHERE _cluster IN ('k3d-k8sql-test-1', 'k3d-k8sql-test-2') AND namespace = 'default' LIMIT 5" 5

# LIMIT 1 should return exactly 1 row even with wildcard cluster
assert_row_count "Multi-cluster LIMIT 1" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE _cluster = '*' LIMIT 1" 1

print_summary
