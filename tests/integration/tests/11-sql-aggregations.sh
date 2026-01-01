#!/usr/bin/env bash
# Comprehensive SQL aggregation function tests for k8sql
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== SQL Aggregation Functions Conformance Tests ==="

# COUNT variations
echo ""
echo "--- COUNT Function ---"

# COUNT(*)
assert_success "COUNT(*) all pods" "k3d-k8sql-test-1" \
    "SELECT COUNT(*) FROM pods"

# COUNT with WHERE clause
assert_success "COUNT(*) with WHERE" "k3d-k8sql-test-1" \
    "SELECT COUNT(*) FROM pods WHERE namespace = 'default'"

# COUNT(column)
assert_success "COUNT(name)" "k3d-k8sql-test-1" \
    "SELECT COUNT(name) FROM pods"

# COUNT(DISTINCT column)
assert_success "COUNT(DISTINCT namespace)" "k3d-k8sql-test-1" \
    "SELECT COUNT(DISTINCT namespace) FROM pods"

# COUNT with GROUP BY
assert_success "COUNT with GROUP BY namespace" "k3d-k8sql-test-1" \
    "SELECT namespace, COUNT(*) as pod_count FROM pods GROUP BY namespace"

# COUNT with HAVING
assert_success "COUNT with HAVING clause" "k3d-k8sql-test-1" \
    "SELECT namespace, COUNT(*) as cnt FROM pods GROUP BY namespace HAVING COUNT(*) > 0"

# MIN/MAX functions
echo ""
echo "--- MIN/MAX Functions ---"

# MIN on integer column
assert_success "MIN(generation)" "k3d-k8sql-test-1" \
    "SELECT MIN(generation) FROM pods"

# MAX on integer column
assert_success "MAX(generation)" "k3d-k8sql-test-1" \
    "SELECT MAX(generation) FROM pods"

# MIN on string column
assert_success "MIN(name)" "k3d-k8sql-test-1" \
    "SELECT MIN(name) FROM namespaces"

# MAX on string column
assert_success "MAX(name)" "k3d-k8sql-test-1" \
    "SELECT MAX(name) FROM namespaces"

# MIN on timestamp
assert_success "MIN(created)" "k3d-k8sql-test-1" \
    "SELECT MIN(created) FROM pods"

# MAX on timestamp
assert_success "MAX(created)" "k3d-k8sql-test-1" \
    "SELECT MAX(created) FROM pods"

# MIN/MAX with GROUP BY
assert_success "MIN/MAX with GROUP BY" "k3d-k8sql-test-1" \
    "SELECT namespace, MIN(generation) as min_gen, MAX(generation) as max_gen FROM pods GROUP BY namespace"

# SUM/AVG functions
echo ""
echo "--- SUM/AVG Functions ---"

# SUM on integer column
assert_success "SUM(generation)" "k3d-k8sql-test-1" \
    "SELECT SUM(generation) FROM pods"

# AVG on integer column
assert_success "AVG(generation)" "k3d-k8sql-test-1" \
    "SELECT AVG(generation) FROM pods"

# SUM with GROUP BY
assert_success "SUM with GROUP BY namespace" "k3d-k8sql-test-1" \
    "SELECT namespace, SUM(generation) as total_gen FROM pods GROUP BY namespace"

# AVG with GROUP BY
assert_success "AVG with GROUP BY namespace" "k3d-k8sql-test-1" \
    "SELECT namespace, AVG(generation) as avg_gen FROM pods GROUP BY namespace"

# AVG with ROUND for precision
assert_success "AVG with ROUND" "k3d-k8sql-test-1" \
    "SELECT ROUND(AVG(generation), 2) as avg_gen FROM pods"

# GROUP BY variations
echo ""
echo "--- GROUP BY Variations ---"

# GROUP BY single column
assert_success "GROUP BY single column" "k3d-k8sql-test-1" \
    "SELECT namespace, COUNT(*) FROM pods GROUP BY namespace"

# GROUP BY multiple columns
assert_success "GROUP BY multiple columns" "k3d-k8sql-test-1" \
    "SELECT namespace, api_version, COUNT(*) FROM pods GROUP BY namespace, api_version"

# GROUP BY with WHERE
assert_success "GROUP BY with WHERE clause" "k3d-k8sql-test-1" \
    "SELECT namespace, COUNT(*) FROM pods WHERE labels->>'app' IS NOT NULL GROUP BY namespace"

# GROUP BY with ORDER BY
assert_success "GROUP BY with ORDER BY count" "k3d-k8sql-test-1" \
    "SELECT namespace, COUNT(*) as cnt FROM pods GROUP BY namespace ORDER BY cnt DESC"

# GROUP BY with LIMIT
assert_success "GROUP BY with LIMIT" "k3d-k8sql-test-1" \
    "SELECT namespace, COUNT(*) FROM pods GROUP BY namespace LIMIT 5"

# HAVING clause
echo ""
echo "--- HAVING Clause ---"

# HAVING with COUNT
assert_success "HAVING with COUNT condition" "k3d-k8sql-test-1" \
    "SELECT namespace, COUNT(*) as cnt FROM pods GROUP BY namespace HAVING COUNT(*) >= 1"

# HAVING with SUM
assert_success "HAVING with SUM condition" "k3d-k8sql-test-1" \
    "SELECT namespace, SUM(generation) as total FROM pods GROUP BY namespace HAVING SUM(generation) > 0"

# HAVING with AVG
assert_success "HAVING with AVG condition" "k3d-k8sql-test-1" \
    "SELECT namespace, AVG(generation) as avg FROM pods GROUP BY namespace HAVING AVG(generation) > 0"

# Multiple aggregations in SELECT
echo ""
echo "--- Multiple Aggregations ---"

# Multiple aggregates on same column
assert_success "Multiple aggregates (MIN, MAX, AVG)" "k3d-k8sql-test-1" \
    "SELECT MIN(generation), MAX(generation), AVG(generation) FROM pods"

# Multiple aggregates on different columns
assert_success "Aggregates on multiple columns" "k3d-k8sql-test-1" \
    "SELECT COUNT(*), COUNT(DISTINCT namespace), MIN(created), MAX(created) FROM pods"

# Aggregates with GROUP BY and multiple metrics
assert_success "Multiple metrics with GROUP BY" "k3d-k8sql-test-1" \
    "SELECT namespace, COUNT(*) as cnt, MIN(generation) as min_gen, MAX(generation) as max_gen, AVG(generation) as avg_gen FROM pods GROUP BY namespace"

# DISTINCT with aggregations
echo ""
echo "--- DISTINCT with Aggregations ---"

# COUNT(DISTINCT)
assert_success "COUNT(DISTINCT namespace)" "k3d-k8sql-test-1" \
    "SELECT COUNT(DISTINCT namespace) FROM pods"

# COUNT(DISTINCT) with GROUP BY
assert_success "COUNT(DISTINCT) with GROUP BY" "k3d-k8sql-test-1" \
    "SELECT namespace, COUNT(DISTINCT api_version) FROM pods GROUP BY namespace"

# Edge cases
echo ""
echo "--- Aggregation Edge Cases ---"

# Aggregation on empty result set
assert_success "COUNT on empty result" "k3d-k8sql-test-1" \
    "SELECT COUNT(*) FROM pods WHERE namespace = 'nonexistent-ns'"

# Aggregation with NULL values
assert_success "COUNT with potential NULLs" "k3d-k8sql-test-1" \
    "SELECT COUNT(deletion_timestamp) FROM pods"

# Aggregation with all NULLs
assert_success "AVG with potential NULLs" "k3d-k8sql-test-1" \
    "SELECT AVG(CAST(NULL AS INTEGER)) FROM pods LIMIT 1"

# ORDER BY aggregate
echo ""
echo "--- ORDER BY with Aggregates ---"

# ORDER BY COUNT DESC
assert_success "ORDER BY COUNT DESC" "k3d-k8sql-test-1" \
    "SELECT namespace, COUNT(*) as cnt FROM pods GROUP BY namespace ORDER BY cnt DESC LIMIT 5"

# ORDER BY multiple aggregates
assert_success "ORDER BY multiple aggregates" "k3d-k8sql-test-1" \
    "SELECT namespace, COUNT(*) as cnt, AVG(generation) as avg FROM pods GROUP BY namespace ORDER BY cnt DESC, avg ASC LIMIT 5"

print_summary
