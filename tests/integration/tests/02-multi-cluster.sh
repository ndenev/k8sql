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
# Debug: show actual output to diagnose multi-cluster issue
echo "DEBUG: IN list query result:"
RUST_LOG=k8sql=debug $K8SQL -c "k3d-k8sql-test-1" -q "SELECT name, _cluster FROM pods WHERE _cluster IN ('k3d-k8sql-test-1', 'k3d-k8sql-test-2') AND namespace = 'default'" -o json 2>&1 || true
echo ""

assert_contains "IN list returns cluster 2 pods" "k3d-k8sql-test-1" \
    "SELECT name, _cluster FROM pods WHERE _cluster IN ('k3d-k8sql-test-1', 'k3d-k8sql-test-2') AND namespace = 'default'" \
    "k3d-k8sql-test-2"

# _cluster column is present in results
assert_contains "_cluster column in output" "k3d-k8sql-test-1" \
    "SELECT _cluster, name FROM pods WHERE namespace = 'default' LIMIT 1" "k3d-k8sql-test-1"

print_summary
