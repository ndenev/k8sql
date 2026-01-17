#!/usr/bin/env bash
# Stress Tests
# Test performance edge cases and resource handling

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== Stress Tests ==="

echo ""
echo "--- Large IN Lists ---"

# Generate a large IN list (50 values)
IN_LIST=$(printf "'ns%d'," {1..50} | sed 's/,$//')
assert_success "Large IN list (50 values)" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace IN ($IN_LIST, 'default', 'kube-system') LIMIT 5"

# Even larger IN list (100 values)
IN_LIST_100=$(printf "'ns%d'," {1..100} | sed 's/,$//')
assert_success "Large IN list (100 values)" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name IN ($IN_LIST_100, 'default')"

echo ""
echo "--- Multi-Cluster Parallel Queries ---"

# Query all clusters in parallel
assert_success "Multi-cluster wildcard" "k3d-k8sql-test-1" \
    "SELECT name, _cluster FROM pods WHERE _cluster = '*' LIMIT 20"

# Multi-cluster with aggregation
assert_success "Multi-cluster aggregation" "k3d-k8sql-test-1" \
    "SELECT _cluster, COUNT(*) AS cnt FROM pods WHERE _cluster = '*' GROUP BY _cluster"

# Multi-cluster with complex filter
assert_success "Multi-cluster complex filter" "k3d-k8sql-test-1" \
    "SELECT name, _cluster FROM pods WHERE _cluster = '*' AND namespace = 'kube-system' LIMIT 10"

echo ""
echo "--- Projection Pushdown ---"

# Minimal projection (should be fast)
assert_success "SELECT name only" "k3d-k8sql-test-1" \
    "SELECT name FROM pods LIMIT 100"

# Full projection (all columns)
assert_success "SELECT * (full projection)" "k3d-k8sql-test-1" \
    "SELECT * FROM pods LIMIT 50"

# Specific JSON columns
assert_success "SELECT specific JSON columns" "k3d-k8sql-test-1" \
    "SELECT name, spec, status FROM pods LIMIT 50"

echo ""
echo "--- Large LIMIT/OFFSET ---"

# Large LIMIT
assert_success "Large LIMIT (1000)" "k3d-k8sql-test-1" \
    "SELECT name FROM pods LIMIT 1000"

# Very large LIMIT (10000)
assert_success "Very large LIMIT (10000)" "k3d-k8sql-test-1" \
    "SELECT name FROM pods LIMIT 10000"

# OFFSET with LIMIT
assert_success "OFFSET with LIMIT" "k3d-k8sql-test-1" \
    "SELECT name FROM pods OFFSET 10 LIMIT 5"

echo ""
echo "--- Many AND Conditions ---"

# Query with many AND conditions
assert_success "Many AND conditions (6)" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace IS NOT NULL AND name IS NOT NULL AND created IS NOT NULL AND api_version IS NOT NULL AND kind IS NOT NULL AND uid IS NOT NULL LIMIT 5"

# More AND conditions with JSON paths
assert_success "Many AND with JSON" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace IS NOT NULL AND status->>'phase' IS NOT NULL AND spec IS NOT NULL LIMIT 5"

echo ""
echo "--- Complex Aggregations ---"

# Multiple aggregations
assert_success "Multiple aggregations" "k3d-k8sql-test-1" \
    "SELECT namespace, COUNT(*) AS total, COUNT(DISTINCT name) AS unique_names FROM pods GROUP BY namespace"

# Aggregation with HAVING
assert_success "Aggregation with HAVING" "k3d-k8sql-test-1" \
    "SELECT namespace, COUNT(*) AS cnt FROM pods GROUP BY namespace HAVING COUNT(*) >= 1"

echo ""
echo "--- Deep JSON Nesting ---"

# Deep JSON path access
assert_success "Deep JSON path" "k3d-k8sql-test-1" \
    "SELECT name, spec->'template'->'spec'->>'serviceAccountName' AS sa FROM deployments LIMIT 5"

# Multiple deep JSON paths
assert_success "Multiple deep JSON paths" "k3d-k8sql-test-1" \
    "SELECT name, spec->>'replicas', status->>'readyReplicas' FROM deployments LIMIT 5"

echo ""
echo "--- Subquery Performance ---"

# Simple subquery
assert_success "Simple subquery" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace IN (SELECT name FROM namespaces WHERE name LIKE 'kube%')"

# Subquery in SELECT (scalar subquery)
assert_success "Scalar subquery" "k3d-k8sql-test-1" \
    "SELECT name, (SELECT COUNT(*) FROM namespaces) AS ns_count FROM pods LIMIT 5"

echo ""
echo "--- Join Performance ---"

# Simple join
assert_success "Simple join" "k3d-k8sql-test-1" \
    "SELECT p.name AS pod, d.name AS deploy FROM pods p JOIN deployments d ON p.namespace = d.namespace LIMIT 10"

# Join with aggregation
assert_success "Join with aggregation" "k3d-k8sql-test-1" \
    "SELECT d.namespace, COUNT(DISTINCT p.name) AS pod_count FROM deployments d LEFT JOIN pods p ON d.namespace = p.namespace GROUP BY d.namespace LIMIT 5"

print_summary
