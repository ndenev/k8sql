#!/usr/bin/env bash
# Metrics API tests for k8sql (requires metrics-server)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== Metrics API Tests ==="

# Check if metrics-server is available
# The metrics.k8s.io API group may not be available in minimal clusters (k3d, kind without metrics-server)
check_metrics_available() {
    local context="$1"
    # Try to access the metrics API - will fail if metrics-server not installed
    kubectl --context "$context" get --raw /apis/metrics.k8s.io/v1beta1 &>/dev/null
}

if ! check_metrics_available "k3d-k8sql-test-1"; then
    echo "SKIP: metrics-server not available in test cluster (this is expected for k3d/kind)"
    echo "      To run metrics tests, install metrics-server or use a cluster with it (EKS, GKE, etc.)"
    print_summary
    exit 0
fi

echo "metrics-server detected, running metrics tests..."

# SHOW TABLES should include podmetrics and nodemetrics
assert_table_contains "SHOW TABLES includes podmetrics" "k3d-k8sql-test-1" \
    "SHOW TABLES" "podmetrics"

assert_table_contains "SHOW TABLES includes nodemetrics" "k3d-k8sql-test-1" \
    "SHOW TABLES" "nodemetrics"

# Basic podmetrics query - should have at least one pod with metrics
assert_min_row_count "SELECT from podmetrics returns results" "k3d-k8sql-test-1" \
    "SELECT name, namespace FROM podmetrics" 1

# Podmetrics specific columns (timestamp, window, containers)
assert_success "Podmetrics has timestamp column" "k3d-k8sql-test-1" \
    "SELECT name, timestamp FROM podmetrics LIMIT 1"

assert_success "Podmetrics has window column" "k3d-k8sql-test-1" \
    "SELECT name, window FROM podmetrics LIMIT 1"

assert_success "Podmetrics has containers column" "k3d-k8sql-test-1" \
    "SELECT name, containers FROM podmetrics LIMIT 1"

# All podmetrics columns together
assert_success "Podmetrics all special columns" "k3d-k8sql-test-1" \
    "SELECT name, namespace, timestamp, window, containers FROM podmetrics LIMIT 3"

# Nodemetrics query - should have exactly 1 node (k3d single-node cluster)
assert_min_row_count "SELECT from nodemetrics returns results" "k3d-k8sql-test-1" \
    "SELECT name FROM nodemetrics" 1

# Nodemetrics specific columns (timestamp, window, usage)
assert_success "Nodemetrics has timestamp column" "k3d-k8sql-test-1" \
    "SELECT name, timestamp FROM nodemetrics LIMIT 1"

assert_success "Nodemetrics has window column" "k3d-k8sql-test-1" \
    "SELECT name, window FROM nodemetrics LIMIT 1"

assert_success "Nodemetrics has usage column" "k3d-k8sql-test-1" \
    "SELECT name, usage FROM nodemetrics LIMIT 1"

# All nodemetrics columns together
assert_success "Nodemetrics all special columns" "k3d-k8sql-test-1" \
    "SELECT name, timestamp, window, usage FROM nodemetrics"

# Namespace filter on podmetrics
assert_success "Podmetrics with namespace filter" "k3d-k8sql-test-1" \
    "SELECT name FROM podmetrics WHERE namespace = 'kube-system'"

# Verify core pods table still works (not overwritten by metrics pods)
assert_contains "Core pods table still has spec column" "k3d-k8sql-test-1" \
    "SELECT name, spec FROM pods WHERE namespace = 'kube-system' LIMIT 1" "containers"

# Verify core nodes table still works (not overwritten by metrics nodes)
assert_success "Core nodes table still has spec/status columns" "k3d-k8sql-test-1" \
    "SELECT name, spec, status FROM nodes LIMIT 1"

# JSON extraction from containers array
assert_success "Extract container metrics from podmetrics" "k3d-k8sql-test-1" \
    "SELECT name, json_get_str(containers, '[0].name') as container FROM podmetrics LIMIT 1"

# JSON extraction from usage object
assert_success "Extract CPU from nodemetrics usage" "k3d-k8sql-test-1" \
    "SELECT name, json_get_str(usage, 'cpu') as cpu FROM nodemetrics LIMIT 1"

print_summary
