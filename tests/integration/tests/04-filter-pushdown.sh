#!/usr/bin/env bash
# Filter pushdown tests for k8sql
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== Filter Pushdown Tests ==="

# Namespace filter pushdown - querying non-existent namespace should be fast
assert_row_count "Namespace filter pushdown (empty result)" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace = 'nonexistent-ns'" 0

# Namespace filter pushdown - kube-system pods
assert_min_row_count "Namespace filter pushdown (kube-system)" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace = 'kube-system'" 1

# Label selector pushdown - exact match
assert_min_row_count "Label selector pushdown" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE labels->>'app' = 'nginx'" 2

# Multiple label selectors
assert_min_row_count "Multiple label selectors" "k3d-k8sql-test-1" \
    "SELECT name FROM deployments WHERE labels->>'app' = 'nginx' AND labels->>'env' = 'test'" 1

# LIKE patterns (client-side, but should still work)
assert_contains "LIKE pattern on name" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name LIKE 'test%'" "test-ns"

# RBAC resources - Role with rules field
assert_contains "Role rules field" "k3d-k8sql-test-1" \
    "SELECT name, rules FROM roles WHERE name = 'test-role' AND namespace = 'default'" "pods"

# RoleBinding subjects field
assert_contains "RoleBinding subjects field" "k3d-k8sql-test-1" \
    "SELECT name, subjects FROM rolebindings WHERE name = 'test-rolebinding' AND namespace = 'default'" "ServiceAccount"

# Combined namespace and label filter
assert_success "Combined namespace and label filter" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace = 'default' AND labels->>'app' = 'nginx'"

# Service selector field
assert_contains "Service spec field" "k3d-k8sql-test-1" \
    "SELECT name, spec FROM services WHERE name = 'test-service' AND namespace = 'default'" "nginx"

# Deployment status
assert_success "Deployment status field" "k3d-k8sql-test-1" \
    "SELECT name, status FROM deployments WHERE name = 'test-app' AND namespace = 'default'"

# Namespace IN list - parallel queries to multiple namespaces
echo ""
echo "=== Namespace IN Parallel Query Tests ==="

# Query pods from multiple namespaces using IN list
assert_success "Namespace IN list query succeeds" "k3d-k8sql-test-1" \
    "SELECT name, namespace FROM pods WHERE namespace IN ('default', 'kube-system')"

# Namespace IN should return pods from both namespaces
assert_contains "Namespace IN returns default pods" "k3d-k8sql-test-1" \
    "SELECT name, namespace FROM pods WHERE namespace IN ('default', 'kube-system')" "default"

assert_contains "Namespace IN returns kube-system pods" "k3d-k8sql-test-1" \
    "SELECT name, namespace FROM pods WHERE namespace IN ('default', 'kube-system')" "kube-system"

# Namespace IN with non-overlapping namespaces
assert_contains "Namespace IN default and test-ns" "k3d-k8sql-test-1" \
    "SELECT DISTINCT namespace FROM pods WHERE namespace IN ('default', 'test-ns')" "default"

# Namespace IN combined with other filters
assert_success "Namespace IN combined with label filter" "k3d-k8sql-test-1" \
    "SELECT name, namespace FROM pods WHERE namespace IN ('default', 'test-ns') AND labels->>'app' = 'nginx'"

# Namespace IN across clusters - should work with _cluster = '*'
assert_success "Namespace IN across all clusters" "k3d-k8sql-test-1" \
    "SELECT name, namespace, _cluster FROM pods WHERE namespace IN ('default', 'kube-system') AND _cluster = '*' LIMIT 10"

# Namespace IN with aggregation
assert_success "Namespace IN with COUNT" "k3d-k8sql-test-1" \
    "SELECT namespace, COUNT(*) as cnt FROM pods WHERE namespace IN ('default', 'kube-system') GROUP BY namespace"

print_summary
