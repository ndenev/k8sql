#!/usr/bin/env bash
# Setup k3d clusters for k8sql integration testing
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Setting up k8sql test clusters ==="

# Check for required tools
for cmd in k3d kubectl; do
    if ! command -v "$cmd" &> /dev/null; then
        echo "Error: $cmd is required but not installed"
        exit 1
    fi
done

# Cluster names
CLUSTER1="k8sql-test-1"
CLUSTER2="k8sql-test-2"

# Clean up any existing test clusters
echo "Cleaning up any existing test clusters..."
k3d cluster delete "$CLUSTER1" 2>/dev/null || true
k3d cluster delete "$CLUSTER2" 2>/dev/null || true

# Create first cluster
echo "Creating cluster 1: $CLUSTER1..."
k3d cluster create "$CLUSTER1" \
    --no-lb \
    --wait \
    --timeout 120s \
    --agents 0

# Create second cluster
echo "Creating cluster 2: $CLUSTER2..."
k3d cluster create "$CLUSTER2" \
    --no-lb \
    --wait \
    --timeout 120s \
    --agents 0

# Merge kubeconfigs
echo "Merging kubeconfigs..."
k3d kubeconfig merge "$CLUSTER1" "$CLUSTER2" --kubeconfig-merge-default

# Wait for clusters to be ready
echo "Waiting for cluster 1 to be ready..."
kubectl --context "k3d-$CLUSTER1" wait --for=condition=Ready nodes --all --timeout=60s

echo "Waiting for cluster 2 to be ready..."
kubectl --context "k3d-$CLUSTER2" wait --for=condition=Ready nodes --all --timeout=60s

# Deploy CRD to both clusters (must be done before resources that use it)
echo "Deploying CRD to cluster 1..."
kubectl --context "k3d-$CLUSTER1" apply -f "$SCRIPT_DIR/fixtures/test-crd.yaml"

echo "Deploying CRD to cluster 2..."
kubectl --context "k3d-$CLUSTER2" apply -f "$SCRIPT_DIR/fixtures/test-crd.yaml"

# Wait for CRD to be established
echo "Waiting for CRD to be established..."
kubectl --context "k3d-$CLUSTER1" wait --for=condition=Established crd/testresources.k8sql.io --timeout=30s
kubectl --context "k3d-$CLUSTER2" wait --for=condition=Established crd/testresources.k8sql.io --timeout=30s

# Deploy test resources to cluster 1
echo "Deploying test resources to cluster 1..."
kubectl --context "k3d-$CLUSTER1" apply -f "$SCRIPT_DIR/fixtures/test-resources.yaml"

# Deploy test resources to cluster 2
echo "Deploying test resources to cluster 2..."
kubectl --context "k3d-$CLUSTER2" apply -f "$SCRIPT_DIR/fixtures/test-resources.yaml"

# Create an extra namespace only in cluster 2 for differentiation
echo "Creating cluster2-only namespace..."
kubectl --context "k3d-$CLUSTER2" create namespace cluster2-only || true

# Wait for deployments to be ready
echo "Waiting for deployments to be ready..."
kubectl --context "k3d-$CLUSTER1" wait --for=condition=available deployment/test-app -n default --timeout=120s
kubectl --context "k3d-$CLUSTER2" wait --for=condition=available deployment/test-app -n default --timeout=120s

echo ""
echo "=== Test clusters ready ==="
echo "Cluster 1: k3d-$CLUSTER1"
echo "Cluster 2: k3d-$CLUSTER2"
echo ""
echo "Contexts available:"
kubectl config get-contexts | grep k8sql-test || true
