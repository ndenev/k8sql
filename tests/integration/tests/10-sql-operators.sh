#!/usr/bin/env bash
# Comprehensive SQL operator tests for k8sql
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== SQL Operators Conformance Tests ==="

# Comparison Operators
echo ""
echo "--- Comparison Operators ---"

# Greater than
assert_min_row_count "Greater than (generation > 0)" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE generation > 0" 1

# Less than
assert_success "Less than (generation < 1000)" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE generation < 1000 LIMIT 5"

# Greater than or equal
assert_success "Greater than or equal (generation >= 1)" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE generation >= 1 LIMIT 5"

# Less than or equal
assert_success "Less than or equal (generation <= 10)" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE generation <= 10 LIMIT 5"

# Not equal
assert_success "Not equal (namespace != 'kube-system')" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace != 'kube-system' LIMIT 5"

# BETWEEN operator
echo ""
echo "--- BETWEEN Operator ---"

assert_success "BETWEEN with integers" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE generation BETWEEN 0 AND 10 LIMIT 5"

# Logical Operators
echo ""
echo "--- Logical Operators (AND/OR/NOT) ---"

# AND with multiple conditions
assert_success "AND with namespace and label" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace = 'default' AND labels->>'app' = 'nginx'"

# OR with multiple conditions
assert_success "OR with namespace conditions" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace = 'default' OR namespace = 'kube-system' LIMIT 5"

# Complex AND/OR combination
assert_success "Complex AND/OR combination" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE (namespace = 'default' OR namespace = 'test-ns') AND labels->>'app' = 'nginx'"

# NOT with IN list
assert_success "NOT IN operator" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name NOT IN ('kube-system', 'kube-public') LIMIT 5"

# NULL Handling
echo ""
echo "--- NULL Operators ---"

# IS NULL (deleted resources have deletionTimestamp)
assert_success "IS NULL check" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE deletion_timestamp IS NULL LIMIT 5"

# IS NOT NULL
assert_min_row_count "IS NOT NULL check" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace IS NOT NULL" 1

# String Operators
echo ""
echo "--- String Operators (LIKE/ILIKE) ---"

# LIKE with wildcard
assert_contains "LIKE with trailing wildcard" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name LIKE 'test%'" "test-ns"

# LIKE with leading wildcard
assert_success "LIKE with leading wildcard" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name LIKE '%system' LIMIT 5"

# LIKE with middle wildcard
assert_success "LIKE with middle wildcard" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name LIKE '%kube%' LIMIT 5"

# NOT LIKE
assert_success "NOT LIKE operator" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name NOT LIKE '%system%' LIMIT 5"

# ILIKE (case-insensitive LIKE)
assert_success "ILIKE case-insensitive match" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name ILIKE '%TEST%' LIMIT 5"

# IN Operator with various types
echo ""
echo "--- IN Operator Edge Cases ---"

# IN with single value (equivalent to =)
assert_contains "IN with single value" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name IN ('default')" "default"

# IN with multiple values
assert_success "IN with multiple values" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name IN ('default', 'kube-system', 'test-ns')"

# IN with no matches
assert_row_count "IN with no matches" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name IN ('nonexistent1', 'nonexistent2')" 0

# Operator Precedence
echo ""
echo "--- Operator Precedence ---"

# AND before OR (test precedence)
assert_success "AND/OR precedence test" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace = 'default' AND labels->>'app' = 'nginx' OR namespace = 'test-ns' LIMIT 5"

# Parentheses override precedence
assert_success "Parentheses override precedence" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace = 'default' AND (labels->>'app' = 'nginx' OR labels->>'app' = 'test-app') LIMIT 5"

# Comparison with JSON operators
echo ""
echo "--- JSON Operator Integration ---"

# JSON arrow operator with comparison
assert_success "JSON ->> with =" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE labels->>'app' = 'nginx' LIMIT 5"

# JSON arrow operator with IN
assert_success "JSON ->> with IN" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE labels->>'app' IN ('nginx', 'test-app') LIMIT 5"

# JSON arrow operator with LIKE
assert_success "JSON ->> with LIKE" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE labels->>'app' LIKE 'ngin%' LIMIT 5"

# JSON arrow operator with IS NOT NULL
assert_success "JSON ->> with IS NOT NULL" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE labels->>'app' IS NOT NULL LIMIT 5"

print_summary
