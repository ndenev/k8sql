#!/usr/bin/env bash
# Tests for fully-qualified names (resource.group) and multi-version CRD handling
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== Fully-Qualified Names and Multi-Version CRD Tests ==="

# --- Multi-Version CRD Tests ---
# The widgets CRD has v1alpha1 (served, not storage) and v1beta1 (served, storage)
# Only v1beta1 should be registered

# Widget table is registered (using storage version)
assert_table_contains "widgets table is registered" "k3d-k8sql-test-1" \
    "SHOW TABLES" "widgets"

# api_version should be v1beta1 (storage version), not v1alpha1
assert_contains "widgets use storage version v1beta1" "k3d-k8sql-test-1" \
    "SELECT DISTINCT api_version FROM widgets" "multiversion.k8sql.io/v1beta1"

assert_not_contains "widgets NOT using v1alpha1" "k3d-k8sql-test-1" \
    "SELECT DISTINCT api_version FROM widgets" "v1alpha1"

# Can query widget instances
assert_row_count "Query widgets in default namespace" "k3d-k8sql-test-1" \
    "SELECT name FROM widgets WHERE namespace = 'default'" 1

assert_contains "Widget instance test-widget-1 exists" "k3d-k8sql-test-1" \
    "SELECT name FROM widgets" "test-widget-1"

# --- Fully-Qualified Name Tests ---
# FQ names use resource.group format (like kubectl)

# Core resources (pods, namespaces, etc.) - can use short name or FQ name
assert_success "Query pods using short name" "k3d-k8sql-test-1" \
    "SELECT name FROM pods LIMIT 1"

# Apps group resources
assert_success "Query deployments using short name" "k3d-k8sql-test-1" \
    "SELECT name FROM deployments LIMIT 1"

# Query using FQ name with quotes (dots require quoting in SQL)
assert_success "Query deployments using FQ name" "k3d-k8sql-test-1" \
    'SELECT name FROM "deployments.apps" LIMIT 1'

assert_success "Query statefulsets using FQ name" "k3d-k8sql-test-1" \
    'SELECT name FROM "statefulsets.apps" LIMIT 1'

assert_success "Query daemonsets using FQ name" "k3d-k8sql-test-1" \
    'SELECT name FROM "daemonsets.apps" LIMIT 1'

assert_success "Query replicasets using FQ name" "k3d-k8sql-test-1" \
    'SELECT name FROM "replicasets.apps" LIMIT 1'

# CRD with FQ name
assert_success "Query CRD using FQ name" "k3d-k8sql-test-1" \
    'SELECT name FROM "testresources.k8sql.io" LIMIT 1'

assert_success "Query widgets using FQ name" "k3d-k8sql-test-1" \
    'SELECT name FROM "widgets.multiversion.k8sql.io" LIMIT 1'

# FQ name returns same data as short name
assert_contains "FQ name returns same data as short name" "k3d-k8sql-test-1" \
    'SELECT name FROM "widgets.multiversion.k8sql.io" WHERE name = '\''test-widget-1'\''' "test-widget-1"

# Short name alias still works
assert_success "Widget short name 'wgt' works" "k3d-k8sql-test-1" \
    "SELECT name FROM wgt LIMIT 1"

# --- Verify api_version is correct for various resources ---
assert_contains "deployments.apps has apps/v1 api_version" "k3d-k8sql-test-1" \
    "SELECT DISTINCT api_version FROM deployments" "apps/v1"

assert_contains "pods has v1 api_version" "k3d-k8sql-test-1" \
    "SELECT DISTINCT api_version FROM pods" "v1"

assert_contains "testresources has k8sql.io/v1 api_version" "k3d-k8sql-test-1" \
    "SELECT DISTINCT api_version FROM testresources" "k8sql.io/v1"

print_summary
