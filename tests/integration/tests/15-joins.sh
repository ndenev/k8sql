#!/usr/bin/env bash
# Comprehensive JOIN tests for k8sql
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== JOIN Operations Conformance Tests ==="

# INNER JOIN
echo ""
echo "--- INNER JOIN ---"

# Join pods with namespaces
assert_success "INNER JOIN pods with namespaces" "k3d-k8sql-test-1" \
    "SELECT p.name as pod_name, n.name as ns_name
     FROM pods p
     INNER JOIN namespaces n ON p.namespace = n.name
     LIMIT 5"

# Join with WHERE clause
assert_success "INNER JOIN with WHERE" "k3d-k8sql-test-1" \
    "SELECT p.name, d.name
     FROM pods p
     INNER JOIN deployments d ON p.namespace = d.namespace
     WHERE p.namespace = 'default'
     LIMIT 5"

# Join on label match (pods to services via selector)
# Note: service selector is a JSON object, need to extract the 'app' field
assert_success "JOIN on label selector" "k3d-k8sql-test-1" \
    "SELECT p.name as pod, s.name as service
     FROM pods p
     INNER JOIN services s ON p.namespace = s.namespace
       AND p.labels->>'app' = s.spec->'selector'->>'app'
     WHERE p.namespace = 'default'
       AND p.labels->>'app' IS NOT NULL
     LIMIT 5"

# Multiple column join
assert_success "JOIN on multiple columns" "k3d-k8sql-test-1" \
    "SELECT p1.name, p2.name
     FROM pods p1
     INNER JOIN pods p2 ON p1.namespace = p2.namespace
       AND p1.labels->>'app' = p2.labels->>'app'
     WHERE p1.name != p2.name
       AND p1.labels->>'app' IS NOT NULL
       AND p2.labels->>'app' IS NOT NULL
     LIMIT 5"

# LEFT JOIN
echo ""
echo "--- LEFT (OUTER) JOIN ---"

# Left join to include all namespaces even without pods
assert_success "LEFT JOIN namespaces to pods" "k3d-k8sql-test-1" \
    "SELECT n.name as namespace, COUNT(p.name) as pod_count
     FROM namespaces n
     LEFT JOIN pods p ON n.name = p.namespace
     GROUP BY n.name
     LIMIT 10"

# Left join with WHERE on right table
assert_success "LEFT JOIN with WHERE on right" "k3d-k8sql-test-1" \
    "SELECT n.name, p.name
     FROM namespaces n
     LEFT JOIN pods p ON n.name = p.namespace
       AND p.labels->>'app' = 'nginx'
     LIMIT 10"

# Left join showing NULLs
assert_success "LEFT JOIN showing unmatched rows" "k3d-k8sql-test-1" \
    "SELECT n.name as ns, COALESCE(p.name, 'no-pods') as pod
     FROM namespaces n
     LEFT JOIN pods p ON n.name = p.namespace
     LIMIT 10"

# RIGHT JOIN
echo ""
echo "--- RIGHT (OUTER) JOIN ---"

# Right join (all pods, even if namespace info is filtered)
assert_success "RIGHT JOIN" "k3d-k8sql-test-1" \
    "SELECT n.name as ns, p.name as pod
     FROM namespaces n
     RIGHT JOIN pods p ON n.name = p.namespace
     LIMIT 10"

# CROSS JOIN
echo ""
echo "--- CROSS JOIN ---"

# Cartesian product (small tables to avoid timeout)
assert_success "CROSS JOIN small result sets" "k3d-k8sql-test-1" \
    "SELECT n1.name, n2.name
     FROM (SELECT name FROM namespaces LIMIT 2) n1
     CROSS JOIN (SELECT name FROM namespaces LIMIT 2) n2
     LIMIT 10"

# Self Join
echo ""
echo "--- SELF JOIN ---"

# Find pods in same namespace
assert_success "SELF JOIN pods in same namespace" "k3d-k8sql-test-1" \
    "SELECT p1.name as pod1, p2.name as pod2
     FROM pods p1
     JOIN pods p2 ON p1.namespace = p2.namespace
     WHERE p1.name < p2.name
     LIMIT 10"

# Find resources with same labels
assert_success "SELF JOIN on label match" "k3d-k8sql-test-1" \
    "SELECT p1.name, p2.name
     FROM pods p1
     JOIN pods p2 ON p1.labels->>'app' = p2.labels->>'app'
     WHERE p1.name != p2.name
       AND p1.labels->>'app' IS NOT NULL
     LIMIT 10"

# Multiple Joins
echo ""
echo "--- Multiple JOINs ---"

# Join three tables
assert_success "JOIN three tables" "k3d-k8sql-test-1" \
    "SELECT n.name as ns, p.name as pod, s.name as service
     FROM namespaces n
     JOIN pods p ON n.name = p.namespace
     JOIN services s ON p.namespace = s.namespace
     WHERE n.name = 'default'
     LIMIT 10"

# Mixed join types
assert_success "Mixed INNER and LEFT JOIN" "k3d-k8sql-test-1" \
    "SELECT n.name, p.name, d.name
     FROM namespaces n
     INNER JOIN pods p ON n.name = p.namespace
     LEFT JOIN deployments d ON p.namespace = d.namespace
     LIMIT 10"

# JOIN with aggregations
echo ""
echo "--- JOINs with Aggregations ---"

# Count pods per namespace using JOIN
assert_success "JOIN with COUNT" "k3d-k8sql-test-1" \
    "SELECT n.name, COUNT(p.name) as pod_count
     FROM namespaces n
     LEFT JOIN pods p ON n.name = p.namespace
     GROUP BY n.name
     ORDER BY pod_count DESC
     LIMIT 10"

# Multiple aggregates with JOIN
assert_success "JOIN with multiple aggregates" "k3d-k8sql-test-1" \
    "SELECT n.name,
            COUNT(p.name) as pod_count,
            COUNT(DISTINCT p.labels->>'app') as app_count
     FROM namespaces n
     LEFT JOIN pods p ON n.name = p.namespace
     GROUP BY n.name
     LIMIT 10"

# JOIN with HAVING
assert_success "JOIN with GROUP BY and HAVING" "k3d-k8sql-test-1" \
    "SELECT n.name, COUNT(p.name) as cnt
     FROM namespaces n
     LEFT JOIN pods p ON n.name = p.namespace
     GROUP BY n.name
     HAVING COUNT(p.name) > 0
     LIMIT 10"

# Kubernetes-Specific Joins
echo ""
echo "--- Kubernetes-Specific JOIN Patterns ---"

# Join deployments with their pods (via owner references)
# Note: This requires parsing owner_references JSON array
assert_success "Deployments to Pods via metadata" "k3d-k8sql-test-1" \
    "SELECT d.name as deployment, p.name as pod
     FROM deployments d
     JOIN pods p ON d.namespace = p.namespace
       AND p.owner_references LIKE '%' || d.name || '%'
     WHERE d.namespace = 'default'
     LIMIT 10"

# Services to Pods via selector labels
assert_success "Services to Pods via labels" "k3d-k8sql-test-1" \
    "SELECT s.name as service, p.name as pod
     FROM services s
     JOIN pods p ON s.namespace = p.namespace
       AND p.labels->>'app' IS NOT NULL
     WHERE s.namespace = 'default'
     LIMIT 10"

# ConfigMaps referenced by Pods
assert_success "ConfigMaps used by Pods" "k3d-k8sql-test-1" \
    "SELECT c.name as configmap, p.name as pod
     FROM configmaps c
     JOIN pods p ON c.namespace = p.namespace
     WHERE c.namespace = 'default'
     LIMIT 10"

# JOIN with JSON operations
echo ""
echo "--- JOINs with JSON Operations ---"

# Join on JSON field equality
assert_success "JOIN on JSON field" "k3d-k8sql-test-1" \
    "SELECT p1.name, p2.name
     FROM pods p1
     JOIN pods p2 ON p1.namespace = p2.namespace
       AND p1.labels->>'env' = p2.labels->>'env'
     WHERE p1.name != p2.name
       AND p1.labels->>'env' IS NOT NULL
     LIMIT 10"

# Join with JSON path in WHERE
assert_success "JOIN with JSON WHERE clause" "k3d-k8sql-test-1" \
    "SELECT p.name, d.name
     FROM pods p
     JOIN deployments d ON p.namespace = d.namespace
     WHERE p.status->>'phase' = 'Running'
       AND d.labels->>'app' IS NOT NULL
     LIMIT 10"

# JOIN Edge Cases
echo ""
echo "--- JOIN Edge Cases ---"

# Join with no matches
assert_row_count "JOIN with no matches" "k3d-k8sql-test-1" \
    "SELECT p.name, n.name
     FROM pods p
     JOIN namespaces n ON p.namespace = 'nonexistent'
       AND n.name = 'also-nonexistent'" 0

# LEFT JOIN showing all unmatched
assert_success "LEFT JOIN all unmatched" "k3d-k8sql-test-1" \
    "SELECT n.name, p.name
     FROM namespaces n
     LEFT JOIN pods p ON n.name = 'nonexistent-ns'
     LIMIT 5"

# Join with DISTINCT
assert_success "JOIN with DISTINCT" "k3d-k8sql-test-1" \
    "SELECT DISTINCT p.namespace
     FROM pods p
     JOIN namespaces n ON p.namespace = n.name
     LIMIT 10"

# Join with ORDER BY from both tables
assert_success "JOIN with ORDER BY both tables" "k3d-k8sql-test-1" \
    "SELECT p.name, n.name
     FROM pods p
     JOIN namespaces n ON p.namespace = n.name
     ORDER BY n.name, p.name
     LIMIT 10"

print_summary
