#!/usr/bin/env bash
# PRQL/SQL Parity Tests
# Verify that equivalent SQL and PRQL queries produce identical results

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== PRQL/SQL Parity Tests ==="

echo ""
echo "--- Basic SELECT Parity ---"

# Note: PRQL generates slightly different SQL, so we compare sorted JSON results
# These tests verify semantic equivalence, not exact SQL matching

compare_sql_prql "SELECT * with LIMIT" "k3d-k8sql-test-1" \
    "SELECT * FROM namespaces ORDER BY name LIMIT 3" \
    "from namespaces | sort name | take 3"

compare_sql_prql "SELECT specific columns" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces ORDER BY name LIMIT 5" \
    "from namespaces | sort name | select {name} | take 5"

echo ""
echo "--- Filter Parity ---"

compare_sql_prql "WHERE equals" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace = 'kube-system' ORDER BY name LIMIT 5" \
    "from pods | filter namespace == \"kube-system\" | sort name | select {name} | take 5"

compare_sql_prql "WHERE not equals" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name != 'default' ORDER BY name LIMIT 5" \
    "from namespaces | filter name != \"default\" | sort name | select {name} | take 5"

echo ""
echo "--- Aggregation Parity ---"

compare_sql_prql "COUNT(*)" "k3d-k8sql-test-1" \
    "SELECT namespace, COUNT(*) AS cnt FROM pods GROUP BY namespace ORDER BY cnt DESC LIMIT 5" \
    "from pods | group namespace (aggregate {cnt = count this}) | sort {-cnt} | take 5"

echo ""
echo "--- Sort Parity ---"

compare_sql_prql "ORDER BY ASC" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces ORDER BY name ASC LIMIT 5" \
    "from namespaces | sort name | select {name} | take 5"

compare_sql_prql "ORDER BY DESC" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces ORDER BY name DESC LIMIT 5" \
    "from namespaces | sort {-name} | select {name} | take 5"

echo ""
echo "--- JSON Path in PRQL ---"

# PRQL now supports the same JSON path syntax as SQL!
# status.phase is automatically converted to s"status->>'phase'" before compilation
assert_success "PRQL with JSON path syntax" "k3d-k8sql-test-1" \
    "from pods | select {name, phase = status.phase} | take 5"

assert_success "PRQL filter with JSON path" "k3d-k8sql-test-1" \
    "from pods | filter status.phase == \"Running\" | select {name} | take 5"

# Also test nested paths and array indexing
assert_success "PRQL nested JSON path" "k3d-k8sql-test-1" \
    "from deployments | select {name, replicas = spec.replicas} | take 5"

assert_success "PRQL array index in JSON path" "k3d-k8sql-test-1" \
    "from pods | select {name, image = spec.containers[0].image} | take 5"

echo ""
echo "--- Complex PRQL Queries ---"

assert_success "PRQL with multiple transforms" "k3d-k8sql-test-1" \
    "from pods | filter namespace == \"kube-system\" | sort name | select {name, namespace} | take 10"

assert_success "PRQL with aggregation and sort" "k3d-k8sql-test-1" \
    "from pods | group namespace (aggregate {total = count this}) | sort {-total} | take 5"

print_summary
