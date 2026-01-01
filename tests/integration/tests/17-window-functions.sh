#!/usr/bin/env bash
# Comprehensive window function tests for k8sql
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== Window Functions Conformance Tests ==="

# ROW_NUMBER
echo ""
echo "--- ROW_NUMBER() Window Function ---"

# Basic ROW_NUMBER
assert_success "ROW_NUMBER() OVER ORDER BY" "k3d-k8sql-test-1" \
    "SELECT name, ROW_NUMBER() OVER (ORDER BY created) as row_num
     FROM pods
     LIMIT 10"

# ROW_NUMBER with PARTITION BY
assert_success "ROW_NUMBER() with PARTITION BY" "k3d-k8sql-test-1" \
    "SELECT name, namespace, ROW_NUMBER() OVER (PARTITION BY namespace ORDER BY created) as row_num
     FROM pods
     LIMIT 10"

# ROW_NUMBER with multiple ORDER BY columns
assert_success "ROW_NUMBER() with multiple ORDER BY" "k3d-k8sql-test-1" \
    "SELECT name, namespace, ROW_NUMBER() OVER (ORDER BY namespace, created DESC) as row_num
     FROM pods
     LIMIT 10"

# RANK
echo ""
echo "--- RANK() Window Function ---"

# Basic RANK
assert_success "RANK() OVER ORDER BY" "k3d-k8sql-test-1" \
    "SELECT name, generation, RANK() OVER (ORDER BY generation) as rank
     FROM pods
     LIMIT 10"

# RANK with PARTITION BY
assert_success "RANK() with PARTITION BY" "k3d-k8sql-test-1" \
    "SELECT name, namespace, generation, RANK() OVER (PARTITION BY namespace ORDER BY generation DESC) as rank
     FROM pods
     LIMIT 10"

# RANK with ties (same generation values)
assert_success "RANK() showing tie behavior" "k3d-k8sql-test-1" \
    "SELECT namespace, generation, RANK() OVER (ORDER BY generation) as rank
     FROM pods
     WHERE generation IS NOT NULL
     LIMIT 10"

# DENSE_RANK
echo ""
echo "--- DENSE_RANK() Window Function ---"

# Basic DENSE_RANK
assert_success "DENSE_RANK() OVER ORDER BY" "k3d-k8sql-test-1" \
    "SELECT name, generation, DENSE_RANK() OVER (ORDER BY generation) as dense_rank
     FROM pods
     LIMIT 10"

# DENSE_RANK with PARTITION BY
assert_success "DENSE_RANK() with PARTITION BY" "k3d-k8sql-test-1" \
    "SELECT name, namespace, DENSE_RANK() OVER (PARTITION BY namespace ORDER BY created) as dense_rank
     FROM pods
     LIMIT 10"

# Compare RANK vs DENSE_RANK
assert_success "RANK vs DENSE_RANK comparison" "k3d-k8sql-test-1" \
    "SELECT generation,
            RANK() OVER (ORDER BY generation) as rank,
            DENSE_RANK() OVER (ORDER BY generation) as dense_rank
     FROM pods
     WHERE generation IS NOT NULL
     LIMIT 10"

# LAG and LEAD
echo ""
echo "--- LAG() and LEAD() Window Functions ---"

# LAG - get previous row value
assert_success "LAG() to get previous value" "k3d-k8sql-test-1" \
    "SELECT name, created, LAG(name) OVER (ORDER BY created) as prev_pod
     FROM pods
     LIMIT 10"

# LAG with PARTITION BY
assert_success "LAG() with PARTITION BY namespace" "k3d-k8sql-test-1" \
    "SELECT name, namespace, created, LAG(name) OVER (PARTITION BY namespace ORDER BY created) as prev_pod_in_ns
     FROM pods
     LIMIT 10"

# LAG with offset
assert_success "LAG() with offset 2" "k3d-k8sql-test-1" \
    "SELECT name, LAG(name, 2) OVER (ORDER BY created) as pod_2_before
     FROM pods
     LIMIT 10"

# LAG with default value
assert_success "LAG() with default value" "k3d-k8sql-test-1" \
    "SELECT name, LAG(name, 1, 'first') OVER (ORDER BY created) as prev_or_first
     FROM pods
     LIMIT 10"

# LEAD - get next row value
assert_success "LEAD() to get next value" "k3d-k8sql-test-1" \
    "SELECT name, created, LEAD(name) OVER (ORDER BY created) as next_pod
     FROM pods
     LIMIT 10"

# LEAD with PARTITION BY
assert_success "LEAD() with PARTITION BY" "k3d-k8sql-test-1" \
    "SELECT name, namespace, LEAD(name) OVER (PARTITION BY namespace ORDER BY created) as next_pod_in_ns
     FROM pods
     LIMIT 10"

# LEAD with offset and default
assert_success "LEAD() with offset and default" "k3d-k8sql-test-1" \
    "SELECT name, LEAD(name, 2, 'last') OVER (ORDER BY created) as pod_2_after
     FROM pods
     LIMIT 10"

# FIRST_VALUE and LAST_VALUE
echo ""
echo "--- FIRST_VALUE() and LAST_VALUE() Window Functions ---"

# FIRST_VALUE
assert_success "FIRST_VALUE() in window" "k3d-k8sql-test-1" \
    "SELECT name, namespace, FIRST_VALUE(name) OVER (PARTITION BY namespace ORDER BY created) as first_pod
     FROM pods
     LIMIT 10"

# LAST_VALUE with proper frame
assert_success "LAST_VALUE() with ROWS frame" "k3d-k8sql-test-1" \
    "SELECT name, namespace,
            LAST_VALUE(name) OVER (
                PARTITION BY namespace
                ORDER BY created
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as last_pod
     FROM pods
     LIMIT 10"

# NTH_VALUE
echo ""
echo "--- NTH_VALUE() Window Function ---"

# Get second value in partition
assert_success "NTH_VALUE() to get 2nd value" "k3d-k8sql-test-1" \
    "SELECT name, namespace,
            NTH_VALUE(name, 2) OVER (
                PARTITION BY namespace
                ORDER BY created
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as second_pod
     FROM pods
     LIMIT 10"

# Window Frame Specifications
echo ""
echo "--- Window Frame Specifications ---"

# ROWS frame
assert_success "ROWS BETWEEN frame" "k3d-k8sql-test-1" \
    "SELECT name, namespace,
            COUNT(*) OVER (
                PARTITION BY namespace
                ORDER BY created
                ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
            ) as nearby_count
     FROM pods
     LIMIT 10"

# RANGE frame
assert_success "RANGE frame with unbounded" "k3d-k8sql-test-1" \
    "SELECT name, generation,
            SUM(generation) OVER (
                ORDER BY generation
                RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as running_sum
     FROM pods
     WHERE generation IS NOT NULL
     LIMIT 10"

# Multiple Window Functions
echo ""
echo "--- Multiple Window Functions in Single Query ---"

# Multiple window functions with same OVER clause
assert_success "Multiple window functions same OVER" "k3d-k8sql-test-1" \
    "SELECT name, namespace, created,
            ROW_NUMBER() OVER (PARTITION BY namespace ORDER BY created) as row_num,
            RANK() OVER (PARTITION BY namespace ORDER BY created) as rank,
            DENSE_RANK() OVER (PARTITION BY namespace ORDER BY created) as dense_rank
     FROM pods
     LIMIT 10"

# Multiple window functions with different OVER clauses
assert_success "Multiple window functions different OVER" "k3d-k8sql-test-1" \
    "SELECT name, namespace, generation,
            ROW_NUMBER() OVER (ORDER BY created) as overall_row,
            ROW_NUMBER() OVER (PARTITION BY namespace ORDER BY created) as ns_row,
            RANK() OVER (ORDER BY generation DESC) as gen_rank
     FROM pods
     LIMIT 10"

# Window Functions with Aggregations
echo ""
echo "--- Window Functions with Aggregations ---"

# Running total
assert_success "Running SUM with window" "k3d-k8sql-test-1" \
    "SELECT name, namespace, generation,
            SUM(generation) OVER (PARTITION BY namespace ORDER BY created) as running_total
     FROM pods
     WHERE generation IS NOT NULL
     LIMIT 10"

# Running average
assert_success "Running AVG with window" "k3d-k8sql-test-1" \
    "SELECT name, namespace, generation,
            AVG(generation) OVER (PARTITION BY namespace ORDER BY created) as running_avg
     FROM pods
     WHERE generation IS NOT NULL
     LIMIT 10"

# Running count
assert_success "Running COUNT with window" "k3d-k8sql-test-1" \
    "SELECT name, namespace,
            COUNT(*) OVER (PARTITION BY namespace ORDER BY created) as running_count
     FROM pods
     LIMIT 10"

# MIN and MAX in window
assert_success "Running MIN and MAX with window" "k3d-k8sql-test-1" \
    "SELECT name, generation,
            MIN(generation) OVER (ORDER BY created) as min_so_far,
            MAX(generation) OVER (ORDER BY created) as max_so_far
     FROM pods
     WHERE generation IS NOT NULL
     LIMIT 10"

# Window Functions with WHERE
echo ""
echo "--- Window Functions with WHERE Clause ---"

# Filter before window function
assert_success "Window function with WHERE filter" "k3d-k8sql-test-1" \
    "SELECT name, namespace, ROW_NUMBER() OVER (ORDER BY created) as row_num
     FROM pods
     WHERE namespace = 'default'
     LIMIT 10"

# Window function with complex WHERE
assert_success "Window function with complex WHERE" "k3d-k8sql-test-1" \
    "SELECT name, namespace, labels->>'app' as app,
            RANK() OVER (PARTITION BY namespace ORDER BY created) as rank
     FROM pods
     WHERE labels->>'app' IS NOT NULL
       AND namespace != 'kube-public'
     LIMIT 10"

# Window Functions with JOINs
echo ""
echo "--- Window Functions with JOINs ---"

# Window function on joined tables
assert_success "Window function with JOIN" "k3d-k8sql-test-1" \
    "SELECT p.name, p.namespace, n.name as ns_name,
            ROW_NUMBER() OVER (PARTITION BY p.namespace ORDER BY p.created) as row_num
     FROM pods p
     JOIN namespaces n ON p.namespace = n.name
     LIMIT 10"

# Multiple window functions on joined result
assert_success "Multiple windows on JOIN result" "k3d-k8sql-test-1" \
    "SELECT p.name as pod, d.name as deployment,
            ROW_NUMBER() OVER (PARTITION BY p.namespace ORDER BY p.created) as pod_row,
            COUNT(*) OVER (PARTITION BY d.name) as pods_per_deployment
     FROM pods p
     JOIN deployments d ON p.namespace = d.namespace
     WHERE p.namespace = 'default'
     LIMIT 10"

# Window Functions with Subqueries
echo ""
echo "--- Window Functions with Subqueries ---"

# Window function in subquery
assert_success "Window function in subquery" "k3d-k8sql-test-1" \
    "SELECT name, namespace, row_num
     FROM (
         SELECT name, namespace, ROW_NUMBER() OVER (PARTITION BY namespace ORDER BY created) as row_num
         FROM pods
     ) numbered
     WHERE row_num <= 3
     LIMIT 10"

# Filter on window function result
assert_success "Filter on RANK result" "k3d-k8sql-test-1" \
    "SELECT name, namespace, rank
     FROM (
         SELECT name, namespace, RANK() OVER (PARTITION BY namespace ORDER BY created) as rank
         FROM pods
     ) ranked
     WHERE rank = 1
     LIMIT 10"

# Window Functions with CTEs
echo ""
echo "--- Window Functions with CTEs ---"

# CTE with window function
assert_success "CTE with window function" "k3d-k8sql-test-1" \
    "WITH ranked_pods AS (
         SELECT name, namespace, RANK() OVER (PARTITION BY namespace ORDER BY created) as rank
         FROM pods
     )
     SELECT name, namespace, rank
     FROM ranked_pods
     WHERE rank <= 2
     LIMIT 10"

# Multiple CTEs with window functions
assert_success "Multiple CTEs with windows" "k3d-k8sql-test-1" \
    "WITH
         pod_rows AS (
             SELECT name, namespace, ROW_NUMBER() OVER (PARTITION BY namespace ORDER BY created) as row_num
             FROM pods
         ),
         ns_counts AS (
             SELECT namespace, COUNT(*) as total FROM pods GROUP BY namespace
         )
     SELECT pr.name, pr.namespace, pr.row_num, nc.total
     FROM pod_rows pr
     JOIN ns_counts nc ON pr.namespace = nc.namespace
     WHERE pr.row_num = 1
     LIMIT 10"

# Edge Cases
echo ""
echo "--- Window Function Edge Cases ---"

# Empty partition
assert_success "Window on filtered empty result" "k3d-k8sql-test-1" \
    "SELECT name, ROW_NUMBER() OVER (ORDER BY created) as row_num
     FROM pods
     WHERE namespace = 'nonexistent-namespace'"

# NULL handling in ORDER BY
assert_success "Window with NULL in ORDER BY" "k3d-k8sql-test-1" \
    "SELECT name, deletion_timestamp,
            ROW_NUMBER() OVER (ORDER BY deletion_timestamp NULLS LAST) as row_num
     FROM pods
     LIMIT 10"

# Window over single partition
assert_success "Window over entire table" "k3d-k8sql-test-1" \
    "SELECT name, ROW_NUMBER() OVER (ORDER BY created) as global_row
     FROM pods
     LIMIT 10"

# Complex Expressions in Window
echo ""
echo "--- Complex Expressions in Window Functions ---"

# Window with JSON field in ORDER BY
assert_success "Window ORDER BY JSON field" "k3d-k8sql-test-1" \
    "SELECT name, labels->>'app' as app,
            ROW_NUMBER() OVER (ORDER BY labels->>'app') as row_num
     FROM pods
     WHERE labels->>'app' IS NOT NULL
     LIMIT 10"

# Window with CASE in ORDER BY
assert_success "Window with CASE expression" "k3d-k8sql-test-1" \
    "SELECT name, namespace,
            ROW_NUMBER() OVER (
                ORDER BY CASE
                    WHEN namespace = 'default' THEN 1
                    WHEN namespace = 'kube-system' THEN 2
                    ELSE 3
                END
            ) as priority_row
     FROM pods
     LIMIT 10"

print_summary
