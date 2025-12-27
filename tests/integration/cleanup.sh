#!/usr/bin/env bash
# Cleanup k3d clusters after integration testing
set -euo pipefail

echo "=== Cleaning up k8sql test clusters ==="

# Cluster names
CLUSTER1="k8sql-test-1"
CLUSTER2="k8sql-test-2"

# Delete clusters (ignore errors if they don't exist)
echo "Deleting cluster 1..."
k3d cluster delete "$CLUSTER1" 2>/dev/null || true

echo "Deleting cluster 2..."
k3d cluster delete "$CLUSTER2" 2>/dev/null || true

echo "=== Cleanup complete ==="
