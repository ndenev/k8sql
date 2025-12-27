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

print_summary
