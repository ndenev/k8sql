#!/usr/bin/env bash
# Comprehensive string function tests for k8sql
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== String Functions Conformance Tests ==="

# Case Conversion
echo ""
echo "--- Case Conversion Functions ---"

# UPPER
assert_success "UPPER function" "k3d-k8sql-test-1" \
    "SELECT name, UPPER(name) as upper_name FROM namespaces LIMIT 5"

# LOWER
assert_success "LOWER function" "k3d-k8sql-test-1" \
    "SELECT name, LOWER(name) as lower_name FROM namespaces LIMIT 5"

# UPPER in WHERE clause
assert_success "UPPER in WHERE" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE UPPER(name) LIKE '%DEFAULT%' LIMIT 5"

# String Manipulation
echo ""
echo "--- String Manipulation ---"

# CONCAT
assert_success "CONCAT function" "k3d-k8sql-test-1" \
    "SELECT CONCAT(namespace, '/', name) as full_name FROM pods LIMIT 5"

# CONCAT with literals
assert_success "CONCAT with literals" "k3d-k8sql-test-1" \
    "SELECT CONCAT('pod:', name) as labeled_name FROM pods LIMIT 5"

# String length
assert_success "LENGTH/CHAR_LENGTH function" "k3d-k8sql-test-1" \
    "SELECT name, LENGTH(name) as name_length FROM namespaces LIMIT 5"

# SUBSTRING
assert_success "SUBSTRING function" "k3d-k8sql-test-1" \
    "SELECT name, SUBSTRING(name, 1, 5) as short_name FROM namespaces LIMIT 5"

# TRIM
assert_success "TRIM function" "k3d-k8sql-test-1" \
    "SELECT TRIM(name) as trimmed FROM namespaces LIMIT 5"

# LTRIM/RTRIM
assert_success "LTRIM function" "k3d-k8sql-test-1" \
    "SELECT LTRIM(name) FROM namespaces LIMIT 5"

assert_success "RTRIM function" "k3d-k8sql-test-1" \
    "SELECT RTRIM(name) FROM namespaces LIMIT 5"

# Pattern Matching
echo ""
echo "--- String Pattern Matching ---"

# LIKE patterns
assert_success "LIKE with % wildcard" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name LIKE 'kube%'"

assert_success "LIKE with _ wildcard" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name LIKE 'kube_______' LIMIT 5"

# STARTS_WITH
assert_success "STARTS_WITH function" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE STARTS_WITH(name, 'kube') LIMIT 5"

# ENDS_WITH (if available)
assert_success "ENDS_WITH function" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE ENDS_WITH(name, 'system') LIMIT 5"

# POSITION/STRPOS
assert_success "POSITION function" "k3d-k8sql-test-1" \
    "SELECT name, POSITION('kube' IN name) as pos FROM namespaces LIMIT 5"

# String Comparison
echo ""
echo "--- String Comparison ---"

# Case-sensitive comparison
assert_success "Case-sensitive string comparison" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name = 'default'"

# Case-insensitive with ILIKE
assert_success "ILIKE case-insensitive" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name ILIKE 'DEFAULT'"

# String ordering
assert_success "ORDER BY string ascending" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces ORDER BY name ASC LIMIT 5"

assert_success "ORDER BY string descending" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces ORDER BY name DESC LIMIT 5"

# String Aggregations
echo ""
echo "--- String Aggregations ---"

# MIN/MAX on strings
assert_success "MIN(string)" "k3d-k8sql-test-1" \
    "SELECT MIN(name) FROM namespaces"

assert_success "MAX(string)" "k3d-k8sql-test-1" \
    "SELECT MAX(name) FROM namespaces"

# COUNT DISTINCT strings
assert_success "COUNT(DISTINCT string)" "k3d-k8sql-test-1" \
    "SELECT COUNT(DISTINCT namespace) FROM pods"

# String Replacement
echo ""
echo "--- String Replacement ---"

# REPLACE function
assert_success "REPLACE function" "k3d-k8sql-test-1" \
    "SELECT name, REPLACE(name, '-', '_') as replaced FROM namespaces LIMIT 5"

# REGEXP_REPLACE (if available)
assert_success "REGEXP_REPLACE function" "k3d-k8sql-test-1" \
    "SELECT name, REGEXP_REPLACE(name, '[0-9]', 'X') as no_digits FROM pods LIMIT 5"

# Edge Cases
echo ""
echo "--- String Edge Cases ---"

# Empty string
assert_success "Empty string comparison" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE name != '' LIMIT 5"

# String with spaces
assert_success "String with spaces" "k3d-k8sql-test-1" \
    "SELECT CONCAT(name, ' ', namespace) as spaced FROM pods LIMIT 5"

# Very long strings (JSON fields)
assert_success "Long string (spec field)" "k3d-k8sql-test-1" \
    "SELECT name, LENGTH(spec) as spec_length FROM pods LIMIT 5"

# NULL string handling
assert_success "NULL string with COALESCE" "k3d-k8sql-test-1" \
    "SELECT name, COALESCE(labels->>'app', 'no-app') as app FROM pods LIMIT 5"

# String concatenation with NULL
assert_success "CONCAT with potential NULL" "k3d-k8sql-test-1" \
    "SELECT name, CONCAT(namespace, '/', COALESCE(labels->>'app', 'unknown')) as path FROM pods LIMIT 5"

print_summary
