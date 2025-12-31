#!/usr/bin/env bash
# Comprehensive JSON operations tests for k8sql
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== JSON Operations Conformance Tests ==="

# JSON Arrow Operator (->>)
echo ""
echo "--- JSON Arrow Operator (->>)  ---"

# Basic JSON field access
assert_success "JSON ->> basic access" "k3d-k8sql-test-1" \
    "SELECT name, labels->>'app' as app FROM pods LIMIT 5"

# Nested JSON path
assert_success "JSON ->> nested path" "k3d-k8sql-test-1" \
    "SELECT name, status->>'phase' as phase FROM pods LIMIT 5"

# JSON field in WHERE clause
assert_success "JSON ->> in WHERE" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE labels->>'app' = 'nginx'"

# JSON field with NULL values
assert_success "JSON ->> with potential NULLs" "k3d-k8sql-test-1" \
    "SELECT name, labels->>'nonexistent' as missing FROM pods LIMIT 5"

# JSON field comparison
assert_success "JSON ->> equality comparison" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE status->>'phase' = 'Running' LIMIT 5"

# JSON field with IN operator
assert_success "JSON ->> with IN operator" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE labels->>'app' IN ('nginx', 'test-app') LIMIT 5"

# JSON field with LIKE operator
assert_success "JSON ->> with LIKE" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE labels->>'app' LIKE 'ngin%' LIMIT 5"

# JSON field IS NOT NULL
assert_success "JSON ->> IS NOT NULL" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE labels->>'app' IS NOT NULL LIMIT 5"

# DataFusion JSON Functions
echo ""
echo "--- DataFusion JSON Functions ---"

# json_get_str (explicit function)
assert_success "json_get_str function" "k3d-k8sql-test-1" \
    "SELECT name, json_get_str(labels, 'app') as app FROM pods WHERE namespace = 'default' LIMIT 5"

# json_get_int (for numeric values)
assert_success "json_get_int function" "k3d-k8sql-test-1" \
    "SELECT name, json_get_int(spec, 'replicas') as replicas FROM deployments WHERE namespace = 'default' LIMIT 5"

# json_get_bool (for boolean values)
assert_success "json_get_bool function" "k3d-k8sql-test-1" \
    "SELECT name, json_get_bool(spec, 'hostNetwork') as host_network FROM pods LIMIT 5"

# Multiple JSON operations in SELECT
echo ""
echo "--- Multiple JSON Operations ---"

# Multiple JSON fields from same column
assert_success "Multiple JSON fields from labels" "k3d-k8sql-test-1" \
    "SELECT name, labels->>'app' as app, labels->>'env' as env FROM pods WHERE namespace = 'default' LIMIT 5"

# JSON fields from different columns
assert_success "JSON from labels and annotations" "k3d-k8sql-test-1" \
    "SELECT name, labels->>'app' as app, annotations->>'description' as desc FROM pods LIMIT 5"

# JSON in SELECT and WHERE
assert_success "JSON in SELECT and WHERE" "k3d-k8sql-test-1" \
    "SELECT name, status->>'phase' as phase FROM pods WHERE status->>'phase' = 'Running' LIMIT 5"

# Complex JSON Paths
echo ""
echo "--- Complex JSON Paths ---"

# Nested object access (spec.containers is an array)
assert_success "Nested spec field" "k3d-k8sql-test-1" \
    "SELECT name, spec FROM pods WHERE namespace = 'default' LIMIT 3"

# Deep nesting
assert_success "Deep JSON nesting" "k3d-k8sql-test-1" \
    "SELECT name, status FROM pods WHERE namespace = 'default' LIMIT 3"

# JSON Arrays
echo ""
echo "--- JSON Array Operations ---"

# json_get_array to access array fields
assert_success "json_get_array function" "k3d-k8sql-test-1" \
    "SELECT name, json_get_array(spec, 'containers') as containers FROM pods WHERE namespace = 'default' LIMIT 3"

# UNNEST for array expansion (if supported)
# Note: This tests if DataFusion's UNNEST works with our JSON arrays
assert_success "UNNEST with json_get_array" "k3d-k8sql-test-1" \
    "SELECT name, json_get_str(UNNEST(json_get_array(spec, 'containers')), 'name') as container_name FROM pods WHERE namespace = 'default' LIMIT 10"

# JSON in Aggregations
echo ""
echo "--- JSON with Aggregations ---"

# COUNT distinct JSON values
assert_success "COUNT(DISTINCT JSON field)" "k3d-k8sql-test-1" \
    "SELECT COUNT(DISTINCT labels->>'app') FROM pods"

# GROUP BY JSON field
assert_success "GROUP BY JSON field" "k3d-k8sql-test-1" \
    "SELECT labels->>'app' as app, COUNT(*) as cnt FROM pods GROUP BY labels->>'app'"

# Aggregate with JSON WHERE
assert_success "COUNT with JSON WHERE" "k3d-k8sql-test-1" \
    "SELECT COUNT(*) FROM pods WHERE status->>'phase' = 'Running'"

# JSON field in ORDER BY
echo ""
echo "--- JSON in ORDER BY ---"

# ORDER BY JSON string field
assert_success "ORDER BY JSON field" "k3d-k8sql-test-1" \
    "SELECT name, labels->>'app' as app FROM pods ORDER BY labels->>'app' LIMIT 5"

# ORDER BY with GROUP BY on JSON
assert_success "ORDER BY with JSON GROUP BY" "k3d-k8sql-test-1" \
    "SELECT labels->>'app' as app, COUNT(*) as cnt FROM pods GROUP BY labels->>'app' ORDER BY cnt DESC LIMIT 5"

# JSON Type Coercion
echo ""
echo "--- JSON Type Handling ---"

# String to integer comparison
assert_success "JSON string as integer" "k3d-k8sql-test-1" \
    "SELECT name FROM deployments WHERE CAST(json_get_str(spec, 'replicas') AS INTEGER) > 0 LIMIT 5"

# JSON NULL vs SQL NULL
assert_success "JSON NULL handling" "k3d-k8sql-test-1" \
    "SELECT name, labels->>'nonexistent' FROM pods WHERE labels->>'nonexistent' IS NULL LIMIT 5"

# Edge Cases
echo ""
echo "--- JSON Edge Cases ---"

# Empty JSON object
assert_success "Empty JSON object field" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE labels = '{}' LIMIT 5"

# Accessing non-existent key
assert_success "Non-existent JSON key" "k3d-k8sql-test-1" \
    "SELECT name, labels->>'this_key_does_not_exist' as missing FROM pods LIMIT 5"

# JSON field with special characters
assert_success "JSON key with dots/dashes" "k3d-k8sql-test-1" \
    "SELECT name, labels->>'app.kubernetes.io/name' as k8s_name FROM pods LIMIT 5"

# Multiple label selectors (K8s specific)
echo ""
echo "--- Kubernetes-Specific JSON Patterns ---"

# Common K8s label patterns
assert_success "K8s app label" "k3d-k8sql-test-1" \
    "SELECT name FROM pods WHERE labels->>'app.kubernetes.io/name' IS NOT NULL LIMIT 5"

# Annotation access
assert_success "K8s annotations" "k3d-k8sql-test-1" \
    "SELECT name, annotations FROM pods LIMIT 5"

# Owner reference in metadata (complex nested structure)
assert_success "Owner references" "k3d-k8sql-test-1" \
    "SELECT name, owner_references FROM pods LIMIT 5"

print_summary
