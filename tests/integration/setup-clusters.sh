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

# Set up kubeconfig for tests (defaults to /tmp/k8sql-test-kubeconfig to avoid touching ~/.kube/config)
export KUBECONFIG="${KUBECONFIG:-/tmp/k8sql-test-kubeconfig}"
echo "Using KUBECONFIG: $KUBECONFIG"

# Merge kubeconfigs to test-specific location
echo "Merging kubeconfigs..."
k3d kubeconfig merge "$CLUSTER1" "$CLUSTER2" --kubeconfig-switch-context=false --output "$KUBECONFIG"

# Wait for clusters to be ready
echo "Waiting for cluster 1 to be ready..."
kubectl --context "k3d-$CLUSTER1" wait --for=condition=Ready nodes --all --timeout=60s

echo "Waiting for cluster 2 to be ready..."
kubectl --context "k3d-$CLUSTER2" wait --for=condition=Ready nodes --all --timeout=60s

# Deploy CRDs (must be done before resources that use them)
echo "Deploying test-crd.yaml to both clusters..."
kubectl --context "k3d-$CLUSTER1" apply -f "$SCRIPT_DIR/fixtures/test-crd.yaml"
kubectl --context "k3d-$CLUSTER2" apply -f "$SCRIPT_DIR/fixtures/test-crd.yaml"

echo "Deploying test-crd-2.yaml to cluster 2 only (different CRDs per cluster test)..."
kubectl --context "k3d-$CLUSTER2" apply -f "$SCRIPT_DIR/fixtures/test-crd-2.yaml"

echo "Deploying multi-version CRD (test-crd-multiversion.yaml) to both clusters..."
kubectl --context "k3d-$CLUSTER1" apply -f "$SCRIPT_DIR/fixtures/test-crd-multiversion.yaml"
kubectl --context "k3d-$CLUSTER2" apply -f "$SCRIPT_DIR/fixtures/test-crd-multiversion.yaml"

# Wait for CRDs to be established
echo "Waiting for CRDs to be established..."
kubectl --context "k3d-$CLUSTER1" wait --for=condition=Established crd/testresources.k8sql.io --timeout=30s
kubectl --context "k3d-$CLUSTER2" wait --for=condition=Established crd/testresources.k8sql.io --timeout=30s
kubectl --context "k3d-$CLUSTER2" wait --for=condition=Established crd/configs.config.k8sql.io --timeout=30s
kubectl --context "k3d-$CLUSTER1" wait --for=condition=Established crd/widgets.multiversion.k8sql.io --timeout=30s
kubectl --context "k3d-$CLUSTER2" wait --for=condition=Established crd/widgets.multiversion.k8sql.io --timeout=30s

# Deploy test resources to cluster 1
echo "Deploying test resources to cluster 1..."
kubectl --context "k3d-$CLUSTER1" apply -f "$SCRIPT_DIR/fixtures/test-resources.yaml"

# Deploy test resources to cluster 2
echo "Deploying test resources to cluster 2..."
kubectl --context "k3d-$CLUSTER2" apply -f "$SCRIPT_DIR/fixtures/test-resources.yaml"

# Deploy Config CRD resources to cluster 2 only
echo "Deploying Config resources to cluster 2 only..."
kubectl --context "k3d-$CLUSTER2" apply -f "$SCRIPT_DIR/fixtures/test-config-resources.yaml" || {
    echo "Failed to deploy Config resources"
    exit 1
}

# Verify Config resources were created
echo "Verifying Config resources in cluster 2..."
kubectl --context "k3d-$CLUSTER2" get configs -A || {
    echo "Config resources not found in cluster 2"
    exit 1
}

# Deploy Widget resources for multi-version CRD testing
echo "Deploying Widget resources to both clusters..."
kubectl --context "k3d-$CLUSTER1" apply -f "$SCRIPT_DIR/fixtures/test-widget-resources.yaml"
kubectl --context "k3d-$CLUSTER2" apply -f "$SCRIPT_DIR/fixtures/test-widget-resources.yaml"

# Verify Widget resources were created
echo "Verifying Widget resources..."
kubectl --context "k3d-$CLUSTER1" get widgets -A || {
    echo "Widget resources not found in cluster 1"
    exit 1
}
kubectl --context "k3d-$CLUSTER2" get widgets -A || {
    echo "Widget resources not found in cluster 2"
    exit 1
}

# Create an extra namespace only in cluster 2 for differentiation
echo "Creating cluster2-only namespace..."
kubectl --context "k3d-$CLUSTER2" create namespace cluster2-only || true

# Wait for deployments to be ready (5 min timeout for image pulls in CI)
echo "Waiting for deployments to be ready..."
kubectl --context "k3d-$CLUSTER1" wait --for=condition=available deployment/test-app -n default --timeout=300s || {
    echo "Cluster 1 deployment failed. Pod status:"
    kubectl --context "k3d-$CLUSTER1" get pods -n default
    kubectl --context "k3d-$CLUSTER1" describe pods -n default -l app=nginx
    exit 1
}
kubectl --context "k3d-$CLUSTER2" wait --for=condition=available deployment/test-app -n default --timeout=300s || {
    echo "Cluster 2 deployment failed. Pod status:"
    kubectl --context "k3d-$CLUSTER2" get pods -n default
    kubectl --context "k3d-$CLUSTER2" describe pods -n default -l app=nginx
    exit 1
}

# Bump deployment generation to test generation >= 0 queries
# Kubernetes increments generation when spec changes
echo "Bumping deployment generation for testing..."
kubectl --context "k3d-$CLUSTER1" patch deployment test-app -n default \
  --type='json' -p='[{"op": "replace", "path": "/spec/replicas", "value": 3}]'
kubectl --context "k3d-$CLUSTER2" patch deployment test-app -n default \
  --type='json' -p='[{"op": "replace", "path": "/spec/replicas", "value": 3}]'

# Wait for the new replica to be ready
kubectl --context "k3d-$CLUSTER1" wait --for=condition=available deployment/test-app -n default --timeout=60s
kubectl --context "k3d-$CLUSTER2" wait --for=condition=available deployment/test-app -n default --timeout=60s

echo ""
echo "=== Test clusters ready ==="
echo "Cluster 1: k3d-$CLUSTER1"
echo "Cluster 2: k3d-$CLUSTER2"
echo ""
echo "Contexts available:"
kubectl config get-contexts | grep k8sql-test || true
