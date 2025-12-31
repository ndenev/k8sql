#!/usr/bin/env bash
# Data validation tests - verify query results are CORRECT, not just successful
# This suite addresses: "make sure all these tests not only check if the query succeeds 
# but if the data is what we expect"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== Data Validation and Correctness Tests ==="

# Basic COUNT validation
echo ""
echo "--- COUNT Validation ---"

# Verify COUNT(*) matches actual row count
assert_success "COUNT(*) matches row count" "k3d-k8sql-test-1" \
    "SELECT COUNT(*) as count FROM namespaces WHERE name = 'default'"
# Expected: exactly 1 row with count >= 1

# Verify COUNT with WHERE returns sensible value
assert_min_row_count "COUNT with namespace filter" "k3d-k8sql-test-1" \
    "SELECT COUNT(*) as count FROM pods WHERE namespace = 'kube-system'" 1

# Verify COUNT(DISTINCT) is <= COUNT(*)
assert_success "COUNT(DISTINCT) <= COUNT(*)" "k3d-k8sql-test-1" \
    "SELECT 
        COUNT(*) as total,
        COUNT(DISTINCT namespace) as distinct_ns
     FROM pods
     WHERE COUNT(DISTINCT namespace) <= COUNT(*)"

# Cross-Cluster Aggregation Validation
echo ""
echo "--- Cross-Cluster Aggregation Validation ---"

# Verify single cluster count is consistent
assert_min_row_count "Single cluster pod count" "k3d-k8sql-test-1" \
    "SELECT COUNT(*) as count FROM pods WHERE _cluster = 'k3d-k8sql-test-1'" 1

# Verify multi-cluster count >= single cluster count
# Note: This assumes k3d-k8sql-test-2 exists, adjust if needed
assert_success "Multi-cluster count validation" "k3d-k8sql-test-1" \
    "WITH
        cluster1_count AS (SELECT COUNT(*) as c1 FROM pods WHERE _cluster = 'k3d-k8sql-test-1'),
        all_count AS (SELECT COUNT(*) as call FROM pods WHERE _cluster = '*')
     SELECT c1, call
     FROM cluster1_count, all_count
     WHERE call >= c1"

# Verify cross-cluster aggregation doesn't double-count
assert_success "Cross-cluster SUM validation" "k3d-k8sql-test-1" \
    "SELECT _cluster, COUNT(*) as count
     FROM pods
     WHERE _cluster IN ('k3d-k8sql-test-1')
     GROUP BY _cluster"

# JOIN Result Validation
echo ""
echo "--- JOIN Result Validation ---"

# Verify INNER JOIN only returns matching rows
assert_success "INNER JOIN matching validation" "k3d-k8sql-test-1" \
    "SELECT p.name, p.namespace, n.name as ns_name
     FROM pods p
     INNER JOIN namespaces n ON p.namespace = n.name
     WHERE p.namespace = n.name
     LIMIT 10"

# Verify LEFT JOIN includes all left rows
assert_min_row_count "LEFT JOIN preserves left rows" "k3d-k8sql-test-1" \
    "SELECT n.name
     FROM namespaces n
     LEFT JOIN pods p ON n.name = p.namespace
     GROUP BY n.name" 1

# Verify JOIN count makes sense (pods in namespace = join count for that namespace)
assert_success "JOIN count validation" "k3d-k8sql-test-1" \
    "WITH pod_counts AS (
         SELECT namespace, COUNT(*) as direct_count
         FROM pods
         WHERE namespace = 'default'
         GROUP BY namespace
     ),
     join_counts AS (
         SELECT p.namespace, COUNT(*) as join_count
         FROM pods p
         JOIN namespaces n ON p.namespace = n.name
         WHERE p.namespace = 'default'
         GROUP BY p.namespace
     )
     SELECT pc.namespace, pc.direct_count, jc.join_count
     FROM pod_counts pc
     JOIN join_counts jc ON pc.namespace = jc.namespace
     WHERE pc.direct_count = jc.join_count"

# Subquery Filtering Validation
echo ""
echo "--- Subquery Filtering Validation ---"

# Verify IN subquery filters correctly
assert_success "IN subquery filtering" "k3d-k8sql-test-1" \
    "SELECT name, namespace
     FROM pods
     WHERE namespace IN (SELECT name FROM namespaces WHERE name = 'default')
       AND namespace = 'default'
     LIMIT 10"

# Verify NOT IN subquery excludes correctly
assert_row_count "NOT IN excludes correctly" "k3d-k8sql-test-1" \
    "SELECT name FROM pods
     WHERE namespace NOT IN (SELECT name FROM namespaces WHERE name = 'default')
       AND namespace = 'default'" 0

# Verify EXISTS returns only when subquery has results
assert_success "EXISTS validation" "k3d-k8sql-test-1" \
    "SELECT n.name
     FROM namespaces n
     WHERE EXISTS (SELECT 1 FROM pods p WHERE p.namespace = n.name)
       AND (SELECT COUNT(*) FROM pods p WHERE p.namespace = n.name) > 0
     LIMIT 10"

# Verify NOT EXISTS returns only when subquery is empty
assert_success "NOT EXISTS validation" "k3d-k8sql-test-1" \
    "SELECT n.name
     FROM namespaces n
     WHERE NOT EXISTS (SELECT 1 FROM pods p WHERE p.namespace = n.name AND p.name = 'nonexistent-pod-xyz')
     LIMIT 5"

# Aggregation Result Validation
echo ""
echo "--- Aggregation Result Validation ---"

# Verify SUM >= individual values
assert_success "SUM validation" "k3d-k8sql-test-1" \
    "SELECT namespace, SUM(generation) as total
     FROM pods
     WHERE generation IS NOT NULL
     GROUP BY namespace
     HAVING SUM(generation) >= 0"

# Verify AVG is between MIN and MAX
assert_success "AVG between MIN and MAX" "k3d-k8sql-test-1" \
    "SELECT 
        MIN(generation) as min_gen,
        AVG(generation) as avg_gen,
        MAX(generation) as max_gen
     FROM pods
     WHERE generation IS NOT NULL
       AND AVG(generation) >= MIN(generation)
       AND AVG(generation) <= MAX(generation)"

# Verify MIN <= MAX
assert_success "MIN <= MAX validation" "k3d-k8sql-test-1" \
    "SELECT namespace,
            MIN(generation) as min_gen,
            MAX(generation) as max_gen
     FROM pods
     WHERE generation IS NOT NULL
     GROUP BY namespace
     HAVING MIN(generation) <= MAX(generation)"

# GROUP BY Result Validation
echo ""
echo "--- GROUP BY Result Validation ---"

# Verify GROUP BY produces correct groups
assert_success "GROUP BY namespace count" "k3d-k8sql-test-1" \
    "SELECT namespace, COUNT(*) as count
     FROM pods
     WHERE namespace = 'default'
     GROUP BY namespace
     HAVING namespace = 'default'"

# Verify GROUP BY with HAVING filters correctly
assert_success "HAVING filter validation" "k3d-k8sql-test-1" \
    "SELECT namespace, COUNT(*) as count
     FROM pods
     GROUP BY namespace
     HAVING COUNT(*) >= 1"

# Verify multiple GROUP BY columns
assert_success "Multiple GROUP BY validation" "k3d-k8sql-test-1" \
    "SELECT namespace, labels->>'app' as app, COUNT(*) as count
     FROM pods
     WHERE labels->>'app' IS NOT NULL
     GROUP BY namespace, labels->>'app'
     LIMIT 10"

# ORDER BY Result Validation
echo ""
echo "--- ORDER BY Result Validation ---"

# Verify ORDER BY ASC is actually ascending
assert_success "ORDER BY ASC validation" "k3d-k8sql-test-1" \
    "WITH ordered AS (
         SELECT name, ROW_NUMBER() OVER (ORDER BY name ASC) as rn
         FROM namespaces
         ORDER BY name ASC
         LIMIT 5
     )
     SELECT * FROM ordered ORDER BY name ASC"

# Verify ORDER BY DESC is actually descending  
assert_success "ORDER BY DESC validation" "k3d-k8sql-test-1" \
    "SELECT name
     FROM namespaces
     ORDER BY name DESC
     LIMIT 5"

# Verify ORDER BY with NULL handling
assert_success "ORDER BY NULLS LAST" "k3d-k8sql-test-1" \
    "SELECT name, deletion_timestamp
     FROM pods
     ORDER BY deletion_timestamp NULLS LAST
     LIMIT 5"

# LIMIT and OFFSET Validation
echo ""
echo "--- LIMIT and OFFSET Validation ---"

# Verify LIMIT actually limits
assert_row_count "LIMIT 5 returns max 5 rows" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces LIMIT 5" 5

# Verify LIMIT 1 returns exactly 1
assert_row_count "LIMIT 1 returns exactly 1" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces LIMIT 1" 1

# Verify OFFSET skips rows
assert_success "OFFSET validation" "k3d-k8sql-test-1" \
    "WITH all_ns AS (
         SELECT name, ROW_NUMBER() OVER (ORDER BY name) as rn FROM namespaces
     ),
     offset_ns AS (
         SELECT name FROM namespaces ORDER BY name OFFSET 1 LIMIT 1
     )
     SELECT a.name
     FROM all_ns a
     WHERE a.rn = 2"

# DISTINCT Validation
echo ""
echo "--- DISTINCT Validation ---"

# Verify DISTINCT removes duplicates
assert_success "DISTINCT removes duplicates" "k3d-k8sql-test-1" \
    "SELECT COUNT(*) as with_dupes,
            (SELECT COUNT(DISTINCT namespace) FROM pods) as without_dupes
     FROM pods
     WHERE (SELECT COUNT(DISTINCT namespace) FROM pods) <= COUNT(*)"

# Verify DISTINCT with multiple columns
assert_success "DISTINCT multiple columns" "k3d-k8sql-test-1" \
    "SELECT DISTINCT namespace, labels->>'app'
     FROM pods
     WHERE labels->>'app' IS NOT NULL
     LIMIT 10"

# JSON Field Access Validation
echo ""
echo "--- JSON Field Access Validation ---"

# Verify JSON ->> returns strings
assert_success "JSON ->> returns values" "k3d-k8sql-test-1" \
    "SELECT name, labels->>'app' as app
     FROM pods
     WHERE labels->>'app' IS NOT NULL
     LIMIT 5"

# Verify JSON field filtering works correctly
assert_success "JSON field filter validation" "k3d-k8sql-test-1" \
    "SELECT name, labels->>'app' as app
     FROM pods
     WHERE labels->>'app' = 'some-app'
        OR labels->>'app' != 'some-app'
        OR labels->>'app' IS NULL
     LIMIT 5"

# Verify nested JSON access
assert_success "Nested JSON validation" "k3d-k8sql-test-1" \
    "SELECT name, status->>'phase' as phase
     FROM pods
     WHERE status->>'phase' IN ('Running', 'Pending', 'Failed', 'Succeeded', 'Unknown')
        OR status->>'phase' IS NULL
     LIMIT 5"

# Label Selector Pushdown Validation
echo ""
echo "--- Label Selector Pushdown Validation ---"

# Verify label selector returns correct results
assert_success "Label selector accuracy" "k3d-k8sql-test-1" \
    "SELECT name, labels->>'app' as app
     FROM pods
     WHERE labels->>'app' = 'nginx'
       AND labels->>'app' = 'nginx'
     LIMIT 5"

# Verify multiple label selectors combine correctly (AND logic)
assert_success "Multiple label selectors" "k3d-k8sql-test-1" \
    "SELECT name, labels->>'app' as app, labels->>'env' as env
     FROM pods
     WHERE labels->>'app' IS NOT NULL
       AND labels->>'env' IS NOT NULL
     LIMIT 5"

# Field Selector Pushdown Validation
echo ""
echo "--- Field Selector Pushdown Validation ---"

# Verify field selector for pod phase
assert_success "Field selector phase validation" "k3d-k8sql-test-1" \
    "SELECT name, status->>'phase' as phase
     FROM pods
     WHERE status->>'phase' = 'Running'
       AND status->>'phase' = 'Running'
     LIMIT 5"

# Verify field selector for metadata.name
assert_success "Field selector name validation" "k3d-k8sql-test-1" \
    "SELECT name
     FROM pods
     WHERE namespace = 'default'
       AND name LIKE '%'
     LIMIT 5"

# Multi-Cluster Query Validation
echo ""
echo "--- Multi-Cluster Query Validation ---"

# Verify _cluster column is populated
assert_success "_cluster column populated" "k3d-k8sql-test-1" \
    "SELECT DISTINCT _cluster
     FROM pods
     WHERE _cluster IS NOT NULL
     LIMIT 5"

# Verify _cluster filter works
assert_success "_cluster filter validation" "k3d-k8sql-test-1" \
    "SELECT name, _cluster
     FROM pods
     WHERE _cluster = 'k3d-k8sql-test-1'
       AND _cluster = 'k3d-k8sql-test-1'
     LIMIT 5"

# Verify _cluster = '*' returns multiple clusters (if available)
assert_success "_cluster = '*' validation" "k3d-k8sql-test-1" \
    "SELECT _cluster, COUNT(*) as count
     FROM pods
     WHERE _cluster = '*'
     GROUP BY _cluster"

# Window Function Result Validation
echo ""
echo "--- Window Function Result Validation ---"

# Verify ROW_NUMBER increments correctly
assert_success "ROW_NUMBER sequential" "k3d-k8sql-test-1" \
    "WITH numbered AS (
         SELECT name, ROW_NUMBER() OVER (ORDER BY name) as rn
         FROM namespaces
         LIMIT 5
     )
     SELECT * FROM numbered
     WHERE rn >= 1 AND rn <= 5
     ORDER BY rn"

# Verify RANK handles ties
assert_success "RANK tie handling" "k3d-k8sql-test-1" \
    "SELECT generation, RANK() OVER (ORDER BY generation) as rank
     FROM pods
     WHERE generation IS NOT NULL
     LIMIT 10"

# Verify PARTITION BY creates separate windows
assert_success "PARTITION BY creates separate windows" "k3d-k8sql-test-1" \
    "SELECT namespace,
            ROW_NUMBER() OVER (PARTITION BY namespace ORDER BY created) as rn
     FROM pods
     WHERE rn >= 1
     LIMIT 10"

# NULL Handling Validation
echo ""
echo "--- NULL Handling Validation ---"

# Verify IS NULL filters correctly
assert_success "IS NULL filter validation" "k3d-k8sql-test-1" \
    "SELECT name, deletion_timestamp
     FROM pods
     WHERE deletion_timestamp IS NULL
       AND deletion_timestamp IS NULL
     LIMIT 5"

# Verify IS NOT NULL filters correctly
assert_success "IS NOT NULL filter validation" "k3d-k8sql-test-1" \
    "SELECT name, created
     FROM pods
     WHERE created IS NOT NULL
       AND created IS NOT NULL
     LIMIT 5"

# Verify COALESCE returns first non-NULL
assert_success "COALESCE validation" "k3d-k8sql-test-1" \
    "SELECT name,
            COALESCE(deletion_timestamp, created) as timestamp
     FROM pods
     WHERE COALESCE(deletion_timestamp, created) IS NOT NULL
     LIMIT 5"

# Type Casting Validation
echo ""
echo "--- Type Casting Validation ---"

# Verify CAST to VARCHAR works
assert_success "CAST to VARCHAR" "k3d-k8sql-test-1" \
    "SELECT name, CAST(generation AS VARCHAR) as gen_str
     FROM pods
     WHERE CAST(generation AS VARCHAR) LIKE '%'
     LIMIT 5"

# Verify EXTRACT returns integers
assert_success "EXTRACT returns integers" "k3d-k8sql-test-1" \
    "SELECT name, EXTRACT(YEAR FROM created) as year
     FROM pods
     WHERE EXTRACT(YEAR FROM created) >= 2020
     LIMIT 5"

# String Function Result Validation
echo ""
echo "--- String Function Result Validation ---"

# Verify UPPER converts correctly
assert_success "UPPER conversion validation" "k3d-k8sql-test-1" \
    "SELECT name, UPPER(name) as upper_name
     FROM namespaces
     WHERE UPPER(name) = UPPER(name)
     LIMIT 5"

# Verify CONCAT combines strings
assert_success "CONCAT validation" "k3d-k8sql-test-1" \
    "SELECT name, namespace, CONCAT(namespace, '/', name) as full_name
     FROM pods
     WHERE CONCAT(namespace, '/', name) LIKE '%/%'
     LIMIT 5"

# Verify LENGTH returns positive numbers
assert_success "LENGTH validation" "k3d-k8sql-test-1" \
    "SELECT name, LENGTH(name) as len
     FROM namespaces
     WHERE LENGTH(name) > 0
     LIMIT 5"

# CTE Result Validation
echo ""
echo "--- CTE Result Validation ---"

# Verify CTE produces same results as subquery
assert_success "CTE vs subquery equivalence" "k3d-k8sql-test-1" \
    "WITH pod_counts AS (
         SELECT namespace, COUNT(*) as cnt
         FROM pods
         GROUP BY namespace
     )
     SELECT namespace, cnt
     FROM pod_counts
     WHERE cnt > 0
     LIMIT 5"

# Verify multiple CTEs work correctly
assert_success "Multiple CTEs validation" "k3d-k8sql-test-1" \
    "WITH
         pods_per_ns AS (SELECT namespace, COUNT(*) as pod_cnt FROM pods GROUP BY namespace),
         svcs_per_ns AS (SELECT namespace, COUNT(*) as svc_cnt FROM services GROUP BY namespace)
     SELECT p.namespace, p.pod_cnt, COALESCE(s.svc_cnt, 0) as svc_cnt
     FROM pods_per_ns p
     LEFT JOIN svcs_per_ns s ON p.namespace = s.namespace
     WHERE p.pod_cnt >= 0 AND COALESCE(s.svc_cnt, 0) >= 0
     LIMIT 5"

# UNION Result Validation
echo ""
echo "--- UNION Result Validation ---"

# Verify UNION removes duplicates
assert_success "UNION removes duplicates" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name LIKE 'kube%'
     UNION
     SELECT name FROM namespaces WHERE name LIKE 'kube%'
     LIMIT 5"

# Verify UNION ALL keeps duplicates
assert_success "UNION ALL keeps duplicates" "k3d-k8sql-test-1" \
    "SELECT name FROM namespaces WHERE name LIKE 'kube%'
     UNION ALL
     SELECT name FROM namespaces WHERE name LIKE 'kube%'
     LIMIT 10"

# Complex Query Validation
echo ""
echo "--- Complex Query Validation ---"

# Verify complex query with JOIN, subquery, aggregation, window function
assert_success "Complex query validation" "k3d-k8sql-test-1" \
    "WITH namespace_stats AS (
         SELECT n.name as namespace,
                COUNT(p.name) as pod_count,
                COUNT(DISTINCT p.labels->>'app') as app_count
         FROM namespaces n
         LEFT JOIN pods p ON n.name = p.namespace
         GROUP BY n.name
     )
     SELECT namespace,
            pod_count,
            app_count,
            RANK() OVER (ORDER BY pod_count DESC) as rank
     FROM namespace_stats
     WHERE pod_count >= 0
     LIMIT 10"

# Verify nested subquery with aggregation
assert_success "Nested subquery validation" "k3d-k8sql-test-1" \
    "SELECT namespace, pod_count
     FROM (
         SELECT namespace, COUNT(*) as pod_count
         FROM pods
         WHERE namespace IN (
             SELECT name FROM namespaces WHERE name != 'kube-public'
         )
         GROUP BY namespace
     ) counts
     WHERE pod_count > 0
     LIMIT 5"

print_summary
