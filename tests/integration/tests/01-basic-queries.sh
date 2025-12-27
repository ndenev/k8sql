#!/usr/bin/env bash
# Basic query tests for k8sql
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== Basic Query Tests ==="

# Basic SELECT from pods (should have at least the nginx pods from deployment)
assert_min_row_count "SELECT pods returns results" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace = 'default'" 2

# Namespace filter
assert_min_row_count "SELECT pods in kube-system" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace = 'kube-system'" 1

# SELECT specific columns
assert_success "SELECT specific columns" "k3d-k8sql-test-1" \
    "SELECT name, namespace, created FROM pods LIMIT 5"

# SHOW TABLES includes pods
assert_table_contains "SHOW TABLES includes pods" "k3d-k8sql-test-1" \
    "SHOW TABLES" "pods"

# SHOW TABLES includes deployments
assert_table_contains "SHOW TABLES includes deployments" "k3d-k8sql-test-1" \
    "SHOW TABLES" "deployments"

# SHOW TABLES includes configmaps
assert_table_contains "SHOW TABLES includes configmaps" "k3d-k8sql-test-1" \
    "SHOW TABLES" "configmaps"

# SHOW DATABASES lists contexts
assert_table_contains "SHOW DATABASES lists contexts" "k3d-k8sql-test-1" \
    "SHOW DATABASES" "k3d-k8sql-test-1"

# Query deployments
assert_contains "Query deployments" "k3d-k8sql-test-1" \
    "SELECT name FROM deployments WHERE namespace = 'default'" "test-app"

# Query services
assert_contains "Query services" "k3d-k8sql-test-1" \
    "SELECT name FROM services WHERE namespace = 'default'" "test-service"

# Query namespaces
assert_contains "Query namespaces" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces" "test-ns"

# ConfigMap data field access
assert_contains "ConfigMap data field" "k3d-k8sql-test-1" \
    "SELECT name, data FROM configmaps WHERE name = 'test-config'" "key1"

# Label filter
assert_min_row_count "Label filter on pods" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE labels->>'app' = 'nginx'" 2

# Aggregate functions
assert_success "COUNT aggregate" "k3d-k8sql-test-1" \
    "SELECT COUNT(*) FROM pods WHERE namespace = 'default'"

# ORDER BY
assert_success "ORDER BY name" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces ORDER BY name LIMIT 5"

# LIMIT
assert_row_count "LIMIT 1" "k3d-k8sql-test-1" \
    "SELECT name FROM pods LIMIT 1" 1

print_summary
