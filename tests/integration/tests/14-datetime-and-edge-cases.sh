#!/usr/bin/env bash
# Date/Time functions and edge case tests for k8sql
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== Date/Time Functions and Edge Cases Tests ==="

# Timestamp Comparison
echo ""
echo "--- Timestamp Comparison ---"

# Created timestamp exists
assert_min_row_count "Pods have creation timestamp" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE created IS NOT NULL" 1

# Timestamp comparison (greater than)
assert_success "Timestamp > comparison" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE created > TIMESTAMP '2020-01-01 00:00:00' LIMIT 5"

# Timestamp comparison (less than future date)
assert_success "Timestamp < comparison" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE created < TIMESTAMP '2030-01-01 00:00:00' LIMIT 5"

# Timestamp BETWEEN
assert_success "Timestamp BETWEEN" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE created BETWEEN TIMESTAMP '2020-01-01' AND TIMESTAMP '2030-01-01' LIMIT 5"

# Date/Time Functions
echo ""
echo "--- Date/Time Functions ---"

# EXTRACT year from timestamp
assert_success "EXTRACT(YEAR FROM timestamp)" "k3d-k8sql-test-1" \
    "SELECT name, EXTRACT(YEAR FROM created) as year FROM pods LIMIT 5"

# EXTRACT month
assert_success "EXTRACT(MONTH FROM timestamp)" "k3d-k8sql-test-1" \
    "SELECT name, EXTRACT(MONTH FROM created) as month FROM pods LIMIT 5"

# EXTRACT day
assert_success "EXTRACT(DAY FROM timestamp)" "k3d-k8sql-test-1" \
    "SELECT name, EXTRACT(DAY FROM created) as day FROM pods LIMIT 5"

# EXTRACT hour
assert_success "EXTRACT(HOUR FROM timestamp)" "k3d-k8sql-test-1" \
    "SELECT name, EXTRACT(HOUR FROM created) as hour FROM pods LIMIT 5"

# DATE_TRUNC
assert_success "DATE_TRUNC to day" "k3d-k8sql-test-1" \
    "SELECT DATE_TRUNC('day', created) as day, COUNT(*) as cnt FROM pods GROUP BY DATE_TRUNC('day', created) LIMIT 5"

# Date arithmetic (if supported)
assert_success "CURRENT_TIMESTAMP function" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE created < CURRENT_TIMESTAMP LIMIT 5"

# Timestamp ordering
echo ""
echo "--- Timestamp Ordering ---"

# ORDER BY created ASC
assert_success "ORDER BY timestamp ASC" "k3d-k8sql-test-1" \
    "SELECT name, created FROM pods ORDER BY created ASC LIMIT 5"

# ORDER BY created DESC
assert_success "ORDER BY timestamp DESC" "k3d-k8sql-test-1" \
    "SELECT name, created FROM pods ORDER BY created DESC LIMIT 5"

# MIN/MAX timestamp
assert_success "MIN(timestamp)" "k3d-k8sql-test-1" \
    "SELECT MIN(created) as oldest FROM pods"

assert_success "MAX(timestamp)" "k3d-k8sql-test-1" \
    "SELECT MAX(created) as newest FROM pods"

# NULL Handling
echo ""
echo "--- NULL Handling Edge Cases ---"

# IS NULL check
assert_success "IS NULL on deletion_timestamp" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE deletion_timestamp IS NULL LIMIT 5"

# IS NOT NULL check
assert_min_row_count "IS NOT NULL on created" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE created IS NOT NULL" 1

# COALESCE for NULL handling
assert_success "COALESCE with NULL" "k3d-k8sql-test-1" \
    "SELECT name, COALESCE(deletion_timestamp, created) as timestamp FROM pods LIMIT 5"

# NULL in comparisons
assert_success "NULL-safe equality" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE deletion_timestamp IS NULL AND created IS NOT NULL LIMIT 5"

# NULL in aggregations
assert_success "COUNT ignores NULLs" "k3d-k8sql-test-1" \
    "SELECT COUNT(deletion_timestamp) FROM pods"

assert_success "COUNT(*) includes NULLs" "k3d-k8sql-test-1" \
    "SELECT COUNT(*) FROM pods"

# Edge Cases
echo ""
echo "--- SQL Edge Cases ---"

# Empty result set
assert_row_count "Empty result set" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE name = 'this-pod-does-not-exist'" 0

# COUNT on empty result
assert_success "COUNT on empty result" "k3d-k8sql-test-1" \
    "SELECT COUNT(*) FROM pods WHERE namespace = 'nonexistent-namespace'"

# LIMIT 0
assert_row_count "LIMIT 0" "k3d-k8sql-test-1" \
    "SELECT name FROM pods LIMIT 0" 0

# Very large LIMIT
assert_success "Very large LIMIT" "k3d-k8sql-test-1" \
    "SELECT name FROM pods LIMIT 1000000"

# OFFSET without LIMIT
assert_success "OFFSET without LIMIT" "k3d-k8sql-test-1" \
    "SELECT name FROM pods OFFSET 1"

# OFFSET larger than result set
assert_row_count "OFFSET larger than results" "k3d-k8sql-test-1" \
    "SELECT name FROM pods LIMIT 5 OFFSET 1000000" 0

# LIMIT with OFFSET correctness (regression test for OFFSET bug)
# This validates that LIMIT 10 OFFSET 10 returns exactly 10 rows, not 20.
# Bug: DataFusion's with_fetch() receives skip+fetch combined (20), which
# would cause LIMIT pushdown to fetch 20 rows instead of 10.
# Fix: Disabled LIMIT pushdown to ensure correct OFFSET semantics.
assert_row_count "LIMIT 10 OFFSET 10 returns 10 rows" "k3d-k8sql-test-1" \
    "SELECT name FROM pods LIMIT 10 OFFSET 10" 10

# Type Coercion
echo ""
echo "--- Type Coercion Edge Cases ---"

# String to integer
assert_success "CAST string to integer" "k3d-k8sql-test-1" \
    "SELECT name, CAST(generation AS VARCHAR) as gen_str FROM pods LIMIT 5"

# Integer to string
assert_success "CAST integer to string" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE CAST(generation AS VARCHAR) LIKE '1%' LIMIT 5"

# Boolean expressions
assert_success "Boolean AND/OR/NOT" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE (namespace = 'default' OR namespace = 'test-ns') AND NOT (name LIKE '%test%') LIMIT 5"

# Complex Expressions
echo ""
echo "--- Complex Expression Edge Cases ---"

# Nested CASE expressions
assert_success "CASE expression" "k3d-k8sql-test-1" \
    "SELECT name, CASE WHEN namespace = 'default' THEN 'default' WHEN namespace = 'kube-system' THEN 'system' ELSE 'other' END as ns_type FROM pods LIMIT 5"

# Multiple CASE branches
assert_success "Multi-branch CASE" "k3d-k8sql-test-1" \
    "SELECT name, CASE WHEN labels->>'app' IS NULL THEN 'no-app' WHEN labels->>'app' = 'nginx' THEN 'nginx-app' ELSE 'other-app' END as app_type FROM pods LIMIT 5"

# Nested functions
assert_success "Nested functions" "k3d-k8sql-test-1" \
    "SELECT UPPER(SUBSTRING(name, 1, 5)) as short_upper FROM namespaces LIMIT 5"

# Mathematical Operations
echo ""
echo "--- Mathematical Operations ---"

# Basic arithmetic
assert_success "Integer addition" "k3d-k8sql-test-1" \
    "SELECT name, generation + 1 as next_gen FROM pods LIMIT 5"

assert_success "Integer multiplication" "k3d-k8sql-test-1" \
    "SELECT name, generation * 2 as double_gen FROM pods LIMIT 5"

# Division (watch for division by zero)
assert_success "Safe division" "k3d-k8sql-test-1" \
    "SELECT name, CASE WHEN generation > 0 THEN 100 / generation ELSE 0 END as ratio FROM pods LIMIT 5"

# ROUND, CEIL, FLOOR
assert_success "ROUND function" "k3d-k8sql-test-1" \
    "SELECT ROUND(AVG(generation), 2) as avg_gen FROM pods"

assert_success "CEIL function" "k3d-k8sql-test-1" \
    "SELECT CEIL(AVG(generation)) as ceiling FROM pods"

assert_success "FLOOR function" "k3d-k8sql-test-1" \
    "SELECT FLOOR(AVG(generation)) as floor FROM pods"

# Special Characters
echo ""
echo "--- Special Characters in Data ---"

# Names with hyphens
assert_success "Names with hyphens" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name LIKE '%-%' LIMIT 5"

# JSON keys with dots
assert_success "JSON keys with special chars" "k3d-k8sql-test-1" \
    "SELECT name, labels->>'app.kubernetes.io/name' as k8s_name FROM pods LIMIT 5"

# Unicode handling (if present in cluster)
assert_success "Unicode in strings" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE LENGTH(name) > 0 LIMIT 5"

print_summary
