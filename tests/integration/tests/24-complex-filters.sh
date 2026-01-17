#!/usr/bin/env bash
# Complex Filter Tests
# Test edge cases in filter handling including contradictory conditions and complex OR chains

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== Complex Filter Tests ==="

echo ""
echo "--- Contradictory Conditions ---"

# These conditions are logically impossible - should return 0 rows
# Note: DataFusion handles this at execution time since k8sql pushes the first filter to K8s API
assert_row_count "Contradictory namespace (AND)" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace = 'default' AND namespace = 'kube-system'" 0

assert_row_count "Contradictory cluster (AND)" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE _cluster = 'k3d-k8sql-test-1' AND _cluster = 'k3d-k8sql-test-2'" 0

# Contradictory label conditions
assert_row_count "Contradictory label values" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE labels->>'app' = 'nginx' AND labels->>'app' = 'redis'" 0

echo ""
echo "--- Complex OR Conditions ---"

# OR between different filter types
assert_success "OR: namespace OR label" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace = 'kube-system' OR labels->>'app' IS NOT NULL LIMIT 10"

# OR with same column (IN-like behavior)
assert_success "OR: multiple namespace values" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace = 'default' OR namespace = 'kube-system' LIMIT 10"

# Nested AND/OR combinations
assert_success "Nested AND/OR" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE (namespace = 'kube-system' AND labels->>'tier' IS NOT NULL) OR namespace = 'default' LIMIT 10"

echo ""
echo "--- NOT IN and IN Combinations ---"

# IN list with multiple values
assert_success "IN list" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name IN ('default', 'kube-system', 'kube-public') ORDER BY name"

# NOT IN list
assert_success "NOT IN list" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name NOT IN ('default', 'kube-system') ORDER BY name LIMIT 5"

# Combining IN with other filters
assert_success "IN combined with AND" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace IN ('default', 'kube-system') AND name IS NOT NULL LIMIT 10"

echo ""
echo "--- Label Selector Edge Cases ---"

# Multiple label conditions with AND
assert_success "Multiple labels AND" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE labels->>'component' IS NOT NULL AND labels->>'tier' IS NOT NULL LIMIT 5"

# Label with special characters in value
assert_success "Label with special chars" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE labels->>'app' != 'test-with-dash' LIMIT 5"

echo ""
echo "--- Field Selector Edge Cases ---"

# Field selector on pods (status.phase is pushable)
assert_success "Field selector status.phase" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE status->>'phase' = 'Running' LIMIT 10"

# Field selector on name (metadata.name is pushable)
assert_success "Field selector on name" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name = 'default'"

# Combined field selectors
assert_success "Multiple field selectors" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE status->>'phase' = 'Running' AND name IS NOT NULL LIMIT 5"

echo ""
echo "--- Null Handling ---"

# IS NULL
assert_success "IS NULL filter" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE labels->>'nonexistent-label' IS NULL LIMIT 5"

# IS NOT NULL
assert_success "IS NOT NULL filter" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace IS NOT NULL LIMIT 5"

# COALESCE with null
assert_success "COALESCE with null" "k3d-k8sql-test-1" \
    "SELECT name, COALESCE(labels->>'app', 'none') AS app_label FROM pods LIMIT 5"

echo ""
echo "--- LIKE Pattern Matching ---"

# LIKE with wildcard
assert_success "LIKE with wildcard" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name LIKE 'kube%' ORDER BY name"

# NOT LIKE
assert_success "NOT LIKE" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name NOT LIKE 'kube%' ORDER BY name LIMIT 5"

# ILIKE (case insensitive)
assert_success "ILIKE" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name ILIKE 'DEFAULT'"

echo ""
echo "--- Empty Result Edge Cases ---"

# Query that definitely returns no results
assert_row_count "Nonexistent namespace" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE namespace = 'this-namespace-does-not-exist-12345'" 0

# Impossible string comparison
assert_row_count "Impossible name pattern" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE name = '' AND name != ''" 0

print_summary
