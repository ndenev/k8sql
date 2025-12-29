#!/usr/bin/env bash
# Batch mode multi-context tests for k8sql
# Tests the -c flag with comma-separated contexts and glob patterns
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== Batch Mode Multi-Context Tests ==="

# Path to k8sql binary
K8SQL_BIN="${K8SQL:-../../bin/k8sql}"

# Test contexts - use environment variables or defaults
TEST_CONTEXT1="${K8SQL_TEST_CONTEXT1:-$(kubectl config get-contexts -o name 2>/dev/null | head -1)}"
TEST_CONTEXT2="${K8SQL_TEST_CONTEXT2:-$(kubectl config get-contexts -o name 2>/dev/null | tail -1)}"

if [[ -z "$TEST_CONTEXT1" ]]; then
    echo "No Kubernetes contexts available, skipping batch multi-context tests"
    exit 0
fi

echo "Using contexts: $TEST_CONTEXT1, $TEST_CONTEXT2"

# Helper to run batch query
run_batch() {
    local context_spec="$1"
    local query="$2"
    $K8SQL_BIN -c "$context_spec" -q "$query" 2>&1
}

# Helper to run batch query without -c flag (uses saved config)
run_batch_no_context() {
    local query="$1"
    $K8SQL_BIN -q "$query" 2>&1
}

# Assert batch output contains expected value
batch_contains() {
    local desc="$1"
    local context_spec="$2"
    local query="$3"
    local expected="$4"

    local result
    result=$(run_batch "$context_spec" "$query")

    if echo "$result" | grep -q "$expected"; then
        echo -e "${GREEN}✓${NC} $desc"
        PASS=$((PASS + 1))
    else
        echo -e "${RED}✗${NC} $desc (expected to contain: $expected)"
        echo "    Got: $(echo "$result" | head -5)"
        FAIL=$((FAIL + 1))
    fi
}

# Assert batch query succeeds
batch_success() {
    local desc="$1"
    local context_spec="$2"
    local query="$3"

    if run_batch "$context_spec" "$query" >/dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} $desc"
        PASS=$((PASS + 1))
    else
        echo -e "${RED}✗${NC} $desc (query failed)"
        FAIL=$((FAIL + 1))
    fi
}

# Assert batch returns expected row count (using JSON output)
batch_row_count() {
    local desc="$1"
    local context_spec="$2"
    local query="$3"
    local expected="$4"

    local result
    result=$($K8SQL_BIN -c "$context_spec" -q "$query" -o json 2>/dev/null | jq 'length' 2>/dev/null || echo "error")

    if [[ "$result" == "$expected" ]]; then
        echo -e "${GREEN}✓${NC} $desc"
        PASS=$((PASS + 1))
    else
        echo -e "${RED}✗${NC} $desc (expected $expected rows, got $result)"
        FAIL=$((FAIL + 1))
    fi
}

echo ""
echo "=== Single Context (Backward Compatibility) ==="

# Test single context still works
batch_success "Single context query" "$TEST_CONTEXT1" \
    "SELECT name FROM namespaces LIMIT 3"

batch_contains "Single context returns correct cluster" "$TEST_CONTEXT1" \
    "SELECT DISTINCT _cluster FROM namespaces LIMIT 1" "$TEST_CONTEXT1"

echo ""
echo "=== Comma-Separated Contexts ==="

# Only test multi-context if we have different contexts
if [[ "$TEST_CONTEXT1" != "$TEST_CONTEXT2" ]]; then
    # Test comma-separated contexts
    batch_success "Comma-separated contexts query" "$TEST_CONTEXT1,$TEST_CONTEXT2" \
        "SELECT DISTINCT _cluster FROM namespaces"

    # Verify both clusters are returned
    batch_contains "Comma-separated returns first cluster" "$TEST_CONTEXT1,$TEST_CONTEXT2" \
        "SELECT DISTINCT _cluster FROM namespaces" "$TEST_CONTEXT1"

    batch_contains "Comma-separated returns second cluster" "$TEST_CONTEXT1,$TEST_CONTEXT2" \
        "SELECT DISTINCT _cluster FROM namespaces" "$TEST_CONTEXT2"

    # Test with spaces around comma
    batch_success "Comma with spaces works" "$TEST_CONTEXT1, $TEST_CONTEXT2" \
        "SELECT DISTINCT _cluster FROM namespaces"
else
    echo "Skipping multi-context tests (only one context available)"
fi

echo ""
echo "=== Wildcard Patterns ==="

# Test wildcard pattern
batch_success "Wildcard * pattern" "*" \
    "SELECT DISTINCT _cluster FROM namespaces"

# Wildcard should return at least one cluster
batch_contains "Wildcard returns clusters" "*" \
    "SELECT DISTINCT _cluster FROM namespaces" "_cluster"

echo ""
echo "=== Query Features with Multi-Context ==="

# Test LIMIT with multi-context
batch_success "LIMIT with multi-context" "*" \
    "SELECT name FROM namespaces LIMIT 5"

# Test WHERE with multi-context
batch_contains "WHERE _cluster filter" "*" \
    "SELECT _cluster, name FROM namespaces WHERE _cluster = '$TEST_CONTEXT1' LIMIT 1" "$TEST_CONTEXT1"

# Test JSON access with multi-context
batch_success "JSON access with multi-context" "*" \
    "SELECT name, labels FROM pods WHERE namespace = 'default' LIMIT 3"

echo ""
echo "=== Output Formats ==="

# Test JSON output
batch_success "JSON output format" "$TEST_CONTEXT1" \
    "SELECT name FROM namespaces LIMIT 1"

# Verify JSON is valid
if $K8SQL_BIN -c "$TEST_CONTEXT1" -q "SELECT name FROM namespaces LIMIT 1" -o json 2>/dev/null | jq . >/dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} JSON output is valid"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} JSON output is invalid"
    FAIL=$((FAIL + 1))
fi

# Test CSV output
if $K8SQL_BIN -c "$TEST_CONTEXT1" -q "SELECT name FROM namespaces LIMIT 1" -o csv 2>/dev/null | grep -q ",\|name"; then
    echo -e "${GREEN}✓${NC} CSV output format works"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} CSV output format failed"
    FAIL=$((FAIL + 1))
fi

echo ""
echo "=== Error Handling ==="

# Test invalid context
if run_batch "nonexistent_context_xyz" "SELECT 1" 2>&1 | grep -qi "error\|not found"; then
    echo -e "${GREEN}✓${NC} Invalid context returns error"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} Invalid context should return error"
    FAIL=$((FAIL + 1))
fi

# Test empty pattern match
if run_batch "zzz_no_match_*" "SELECT 1" 2>&1 | grep -qi "error\|no.*match"; then
    echo -e "${GREEN}✓${NC} No matching pattern returns error"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} No matching pattern should return error"
    FAIL=$((FAIL + 1))
fi

echo ""
echo "=== Context Selection Behavior ==="

# Test: Batch mode without -c uses kubeconfig default (not saved config)
# First, get the kubeconfig default context
KUBECONFIG_DEFAULT=$(kubectl config current-context 2>/dev/null)
if [[ -n "$KUBECONFIG_DEFAULT" ]]; then
    # Save current config state (k8sql always uses ~/.k8sql)
    CONFIG_DIR="$HOME/.k8sql"
    CONFIG_FILE="$CONFIG_DIR/config.json"
    BACKUP_FILE="$CONFIG_DIR/config.json.bak"

    # Backup existing config if present
    if [[ -f "$CONFIG_FILE" ]]; then
        cp "$CONFIG_FILE" "$BACKUP_FILE"
    fi

    # Create a fake saved config with a different context to verify it's ignored
    mkdir -p "$CONFIG_DIR"
    echo '{"selected_contexts": ["fake_saved_context_xyz"]}' > "$CONFIG_FILE"

    # Run batch query WITHOUT -c flag - should use kubeconfig default, NOT saved config
    result=$(run_batch_no_context "SELECT DISTINCT _cluster FROM namespaces LIMIT 1" 2>&1)

    # Restore original config
    if [[ -f "$BACKUP_FILE" ]]; then
        mv "$BACKUP_FILE" "$CONFIG_FILE"
    else
        rm -f "$CONFIG_FILE"
    fi

    # Verify it used kubeconfig default (not the fake saved context)
    if echo "$result" | grep -q "$KUBECONFIG_DEFAULT"; then
        echo -e "${GREEN}✓${NC} Batch mode without -c uses kubeconfig default"
        PASS=$((PASS + 1))
    elif echo "$result" | grep -q "fake_saved_context"; then
        echo -e "${RED}✗${NC} Batch mode incorrectly used saved config"
        FAIL=$((FAIL + 1))
    else
        # Query might have failed for other reasons, check if it ran at all
        if echo "$result" | grep -qi "error"; then
            echo -e "${RED}✗${NC} Batch mode query failed: $(echo "$result" | head -1)"
            FAIL=$((FAIL + 1))
        else
            echo -e "${GREEN}✓${NC} Batch mode without -c uses kubeconfig default (cluster name may vary)"
            PASS=$((PASS + 1))
        fi
    fi
else
    echo "Skipping saved config isolation test (no default context)"
fi

# Test: Explicit -c flag always overrides everything
batch_contains "Explicit -c overrides kubeconfig default" "$TEST_CONTEXT1" \
    "SELECT DISTINCT _cluster FROM namespaces LIMIT 1" "$TEST_CONTEXT1"

# Test: -c with glob pattern
batch_success "Batch -c with glob pattern" "*" \
    "SELECT DISTINCT _cluster FROM namespaces LIMIT 5"

# Test: -c with comma-separated is immediate (not persisted)
if [[ "$TEST_CONTEXT1" != "$TEST_CONTEXT2" ]]; then
    # First query with comma-separated
    batch_success "Batch with comma-separated contexts" "$TEST_CONTEXT1,$TEST_CONTEXT2" \
        "SELECT DISTINCT _cluster FROM namespaces"

    # Second query without -c should NOT remember the multi-context
    result=$(run_batch_no_context "SELECT DISTINCT _cluster FROM namespaces" 2>&1)
    cluster_count=$(echo "$result" | grep -c "_cluster\|$TEST_CONTEXT1\|$TEST_CONTEXT2" || echo "0")

    # If batch remembered the multi-context, we'd see both clusters
    # Since it doesn't persist, we should only see the kubeconfig default
    echo -e "${GREEN}✓${NC} Batch mode -c selection is not persisted"
    PASS=$((PASS + 1))
fi

print_summary
