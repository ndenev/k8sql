#!/usr/bin/env bash
# Comprehensive subquery tests for k8sql
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== Subquery Operations Conformance Tests ==="

# Scalar Subqueries in WHERE
echo ""
echo "--- Scalar Subqueries in WHERE ---"

# IN subquery
assert_success "IN with subquery" "k3d-k8sql-test-1" \
    "SELECT name FROM pods
     WHERE namespace IN (SELECT name FROM namespaces WHERE name != 'kube-public')
     LIMIT 10"

# NOT IN subquery
assert_success "NOT IN with subquery" "k3d-k8sql-test-1" \
    "SELECT name FROM pods
     WHERE namespace NOT IN (SELECT name FROM namespaces WHERE name = 'kube-public')
     LIMIT 10"

# Subquery with aggregation
assert_success "Subquery with COUNT" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces
     WHERE name IN (
       SELECT namespace FROM pods
       WHERE labels->>'app' IS NOT NULL
     )
     LIMIT 10"

# Subquery in SELECT
echo ""
echo "--- Scalar Subqueries in SELECT ---"

# Scalar subquery returning single value
assert_success "Scalar subquery in SELECT" "k3d-k8sql-test-1" \
    "SELECT name,
            (SELECT COUNT(*) FROM pods p WHERE p.namespace = n.name) as pod_count
     FROM namespaces n
     LIMIT 10"

# Multiple scalar subqueries
assert_success "Multiple scalar subqueries" "k3d-k8sql-test-1" \
    "SELECT name,
            (SELECT COUNT(*) FROM pods p WHERE p.namespace = n.name) as pods,
            (SELECT COUNT(*) FROM services s WHERE s.namespace = n.name) as services
     FROM namespaces n
     LIMIT 5"

# Correlated Subqueries
echo ""
echo "--- Correlated Subqueries ---"

# Correlated subquery in WHERE
assert_success "Correlated subquery in WHERE" "k3d-k8sql-test-1" \
    "SELECT name FROM pods p
     WHERE EXISTS (
       SELECT 1 FROM namespaces n
       WHERE n.name = p.namespace
         AND n.labels->>'env' = 'production'
     )
     LIMIT 10"

# Correlated NOT EXISTS
assert_success "Correlated NOT EXISTS" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces n
     WHERE NOT EXISTS (
       SELECT 1 FROM pods p
       WHERE p.namespace = n.name
         AND p.labels->>'app' = 'nginx'
     )
     LIMIT 10"

# Correlated subquery with aggregation
assert_success "Correlated subquery with COUNT" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces n
     WHERE (SELECT COUNT(*) FROM pods p WHERE p.namespace = n.name) > 0
     LIMIT 10"

# EXISTS and NOT EXISTS
echo ""
echo "--- EXISTS and NOT EXISTS ---"

# EXISTS
assert_success "EXISTS operator" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces n
     WHERE EXISTS (SELECT 1 FROM pods p WHERE p.namespace = n.name)
     LIMIT 10"

# NOT EXISTS
assert_success "NOT EXISTS operator" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces n
     WHERE NOT EXISTS (SELECT 1 FROM pods p WHERE p.namespace = n.name)
     LIMIT 10"

# EXISTS with complex condition
assert_success "EXISTS with JSON condition" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces n
     WHERE EXISTS (
       SELECT 1 FROM pods p
       WHERE p.namespace = n.name
         AND p.status->>'phase' = 'Running'
     )
     LIMIT 10"

# Derived Tables (FROM subquery)
echo ""
echo "--- Derived Tables (Subquery in FROM) ---"

# Simple derived table
assert_success "Derived table in FROM" "k3d-k8sql-test-1" \
    "SELECT ns_name, pod_count
     FROM (
       SELECT namespace as ns_name, COUNT(*) as pod_count
       FROM pods
       GROUP BY namespace
     ) as pod_counts
     LIMIT 10"

# Derived table with JOIN
assert_success "JOIN with derived table" "k3d-k8sql-test-1" \
    "SELECT n.name, pc.pod_count
     FROM namespaces n
     JOIN (
       SELECT namespace, COUNT(*) as pod_count
       FROM pods
       GROUP BY namespace
     ) pc ON n.name = pc.namespace
     LIMIT 10"

# Multiple derived tables
assert_success "Multiple derived tables" "k3d-k8sql-test-1" \
    "SELECT pods.ns, services.ns, pods.cnt, services.cnt
     FROM (
       SELECT namespace as ns, COUNT(*) as cnt FROM pods GROUP BY namespace
     ) pods
     JOIN (
       SELECT namespace as ns, COUNT(*) as cnt FROM services GROUP BY namespace
     ) services ON pods.ns = services.ns
     LIMIT 5"

# Nested Subqueries
echo ""
echo "--- Nested Subqueries ---"

# Two levels of nesting
assert_success "Nested subqueries (2 levels)" "k3d-k8sql-test-1" \
    "SELECT name FROM pods
     WHERE namespace IN (
       SELECT name FROM namespaces
       WHERE name IN (
         SELECT DISTINCT namespace FROM pods WHERE labels->>'app' = 'nginx'
       )
     )
     LIMIT 10"

# Three levels of nesting
assert_success "Nested subqueries (3 levels)" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces
     WHERE name IN (
       SELECT namespace FROM pods
       WHERE name IN (
         SELECT name FROM pods
         WHERE labels->>'app' IN (
           SELECT DISTINCT labels->>'app' FROM deployments
           WHERE namespace = 'default'
         )
       )
     )
     LIMIT 5"

# Subqueries with Aggregations
echo ""
echo "--- Subqueries with Aggregations ---"

# Subquery in HAVING
assert_success "Subquery in HAVING" "k3d-k8sql-test-1" \
    "SELECT namespace, COUNT(*) as cnt
     FROM pods
     GROUP BY namespace
     HAVING COUNT(*) > (SELECT AVG(pod_count) FROM (
       SELECT COUNT(*) as pod_count FROM pods GROUP BY namespace
     ))
     LIMIT 10"

# Comparison with subquery aggregate
assert_success "Compare with subquery MAX" "k3d-k8sql-test-1" \
    "SELECT name, generation
     FROM pods
     WHERE generation = (SELECT MAX(generation) FROM pods)
     LIMIT 5"

# Comparison with subquery MIN
assert_success "Compare with subquery MIN" "k3d-k8sql-test-1" \
    "SELECT name, created
     FROM pods
     WHERE created = (SELECT MIN(created) FROM pods)
     LIMIT 5"

# Subqueries with UNION
echo ""
echo "--- Subqueries with UNION ---"

# UNION of subqueries
assert_success "UNION of subqueries" "k3d-k8sql-test-1" \
    "SELECT name, 'namespace' as type FROM namespaces WHERE name LIKE 'kube%'
     UNION
     SELECT name, 'pod' as type FROM pods WHERE namespace = 'default'
     LIMIT 10"

# UNION ALL
assert_success "UNION ALL" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name LIKE 'kube%'
     UNION ALL
     SELECT namespace FROM pods WHERE namespace LIKE 'kube%'
     LIMIT 10"

# Complex Subquery Patterns
echo ""
echo "--- Complex Subquery Patterns ---"

# Subquery with JSON operations
assert_success "Subquery with JSON" "k3d-k8sql-test-1" \
    "SELECT name FROM pods
     WHERE labels->>'app' IN (
       SELECT DISTINCT labels->>'app'
       FROM deployments
       WHERE namespace = 'default'
         AND labels->>'app' IS NOT NULL
     )
     LIMIT 10"

# Subquery with multiple conditions
assert_success "Subquery with multiple WHERE" "k3d-k8sql-test-1" \
    "SELECT name FROM pods p
     WHERE namespace IN (
       SELECT name FROM namespaces
       WHERE name != 'kube-public'
         AND name != 'kube-node-lease'
     )
     AND labels->>'app' IS NOT NULL
     LIMIT 10"

# ANY/ALL operators (if supported)
echo ""
echo "--- ANY and ALL Operators ---"

# = ANY (equivalent to IN)
assert_success "= ANY operator" "k3d-k8sql-test-1" \
    "SELECT name FROM pods
     WHERE namespace = ANY(SELECT name FROM namespaces WHERE name LIKE 'kube%')
     LIMIT 10"

# != ALL (equivalent to NOT IN)
assert_success "!= ALL operator" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces
     WHERE name != ALL(SELECT DISTINCT namespace FROM pods WHERE namespace LIKE 'kube%')
     LIMIT 5"

# > ANY
assert_success "> ANY operator" "k3d-k8sql-test-1" \
    "SELECT name, generation FROM pods
     WHERE generation > ANY(SELECT generation FROM pods WHERE namespace = 'default')
     LIMIT 5"

# < ALL
assert_success "< ALL operator" "k3d-k8sql-test-1" \
    "SELECT name, generation FROM pods
     WHERE generation < ALL(SELECT MAX(generation) FROM pods GROUP BY namespace)
     LIMIT 5"

# Common Table Expressions (CTEs)
echo ""
echo "--- CTEs (WITH clause) ---"

# Simple CTE
assert_success "Simple CTE" "k3d-k8sql-test-1" \
    "WITH pod_counts AS (
       SELECT namespace, COUNT(*) as cnt
       FROM pods
       GROUP BY namespace
     )
     SELECT * FROM pod_counts
     WHERE cnt > 0
     LIMIT 10"

# Multiple CTEs
assert_success "Multiple CTEs" "k3d-k8sql-test-1" \
    "WITH
       pod_counts AS (SELECT namespace, COUNT(*) as pod_cnt FROM pods GROUP BY namespace),
       svc_counts AS (SELECT namespace, COUNT(*) as svc_cnt FROM services GROUP BY namespace)
     SELECT p.namespace, p.pod_cnt, COALESCE(s.svc_cnt, 0) as svc_cnt
     FROM pod_counts p
     LEFT JOIN svc_counts s ON p.namespace = s.namespace
     LIMIT 10"

# Recursive CTE (if supported)
# Note: May not be supported, but worth testing
assert_success "CTE with JOIN" "k3d-k8sql-test-1" \
    "WITH running_pods AS (
       SELECT name, namespace
       FROM pods
       WHERE status->>'phase' = 'Running'
     )
     SELECT n.name, COUNT(rp.name) as running_count
     FROM namespaces n
     LEFT JOIN running_pods rp ON n.name = rp.namespace
     GROUP BY n.name
     LIMIT 10"

# Edge Cases
echo ""
echo "--- Subquery Edge Cases ---"

# Empty subquery result
assert_row_count "Empty subquery IN" "k3d-k8sql-test-1" \
    "SELECT name FROM pods
     WHERE namespace IN (SELECT name FROM namespaces WHERE name = 'nonexistent')" 0

# Subquery returning NULL
assert_success "Subquery with NULL handling" "k3d-k8sql-test-1" \
    "SELECT name FROM pods
     WHERE namespace IN (
       SELECT name FROM namespaces
       WHERE name IS NOT NULL
     )
     LIMIT 10"

# Subquery with LIMIT
assert_success "Subquery with LIMIT" "k3d-k8sql-test-1" \
    "SELECT name FROM pods
     WHERE namespace IN (SELECT name FROM namespaces LIMIT 5)
     LIMIT 10"

# Subquery in ORDER BY
assert_success "Subquery in ORDER BY" "k3d-k8sql-test-1" \
    "SELECT name,
            (SELECT COUNT(*) FROM pods p WHERE p.namespace = n.name) as cnt
     FROM namespaces n
     ORDER BY cnt DESC
     LIMIT 5"

print_summary
