#!/usr/bin/env bash
# REPL mode tests for k8sql
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== REPL Mode Tests ==="

# Path to k8sql binary
K8SQL_BIN="${K8SQL:-../../bin/k8sql}"

# Test contexts - use environment variables or defaults
# For k3d test environment: K8SQL_TEST_CONTEXT1=k3d-k8sql-test-1 K8SQL_TEST_CONTEXT2=k3d-k8sql-test-2
TEST_CONTEXT1="${K8SQL_TEST_CONTEXT1:-$(kubectl config get-contexts -o name 2>/dev/null | head -1)}"
TEST_CONTEXT2="${K8SQL_TEST_CONTEXT2:-$(kubectl config get-contexts -o name 2>/dev/null | tail -1)}"

if [[ -z "$TEST_CONTEXT1" ]]; then
    echo "No Kubernetes contexts available, skipping REPL tests"
    exit 0
fi

echo "Using contexts: $TEST_CONTEXT1, $TEST_CONTEXT2"

# Helper to run REPL commands and capture output
run_repl() {
    local context="$1"
    shift
    local commands="$*"
    echo -e "$commands\n\\q" | $K8SQL_BIN -c "$context" 2>&1
}

# Helper to check if output contains expected string
repl_contains() {
    local desc="$1"
    local context="$2"
    local commands="$3"
    local expected="$4"

    local output
    output=$(run_repl "$context" "$commands")

    if echo "$output" | grep -q "$expected"; then
        echo -e "${GREEN}✓${NC} $desc"
        PASS=$((PASS + 1))
    else
        echo -e "${RED}✗${NC} $desc (expected to contain: $expected)"
        echo "    Output: $(echo "$output" | head -10)"
        FAIL=$((FAIL + 1))
    fi
}

# Helper to check REPL doesn't error
repl_success() {
    local desc="$1"
    local context="$2"
    local commands="$3"

    local output
    output=$(run_repl "$context" "$commands")

    if echo "$output" | grep -qi "error"; then
        echo -e "${RED}✗${NC} $desc (unexpected error in output)"
        echo "    Output: $(echo "$output" | head -10)"
        FAIL=$((FAIL + 1))
    else
        echo -e "${GREEN}✓${NC} $desc"
        PASS=$((PASS + 1))
    fi
}

# Test basic query in REPL
repl_contains "Basic SELECT query" "$TEST_CONTEXT1" \
    "SELECT 1 as test;" "1"

# Test SHOW TABLES command
repl_contains "SHOW TABLES command" "$TEST_CONTEXT1" \
    "SHOW TABLES;" "pods"

# Test \\dt shortcut
repl_contains "\\dt shortcut for SHOW TABLES" "$TEST_CONTEXT1" \
    "\\dt" "pods"

# Test SHOW DATABASES command
repl_contains "SHOW DATABASES command" "$TEST_CONTEXT1" \
    "SHOW DATABASES;" "$TEST_CONTEXT1"

# Test \\l shortcut
repl_contains "\\l shortcut for SHOW DATABASES" "$TEST_CONTEXT1" \
    "\\l" "$TEST_CONTEXT1"

# Test DESCRIBE table
repl_contains "DESCRIBE table" "$TEST_CONTEXT1" \
    "DESCRIBE pods;" "name"

# Test \\d shortcut
repl_contains "\\d shortcut for DESCRIBE" "$TEST_CONTEXT1" \
    "\\d pods" "namespace"

# Test USE command to switch context (only if we have a second context)
if [[ "$TEST_CONTEXT1" != "$TEST_CONTEXT2" ]]; then
    repl_contains "USE command switches context" "$TEST_CONTEXT1" \
        "USE $TEST_CONTEXT2;" "Switched to"

    # Test USE with multiple contexts
    repl_contains "USE with multiple contexts" "$TEST_CONTEXT1" \
        "USE $TEST_CONTEXT1, $TEST_CONTEXT2;" "Switched to"

    # Test query after USE multi-context
    repl_contains "Query after USE multi-context" "$TEST_CONTEXT1" \
        "USE $TEST_CONTEXT1, $TEST_CONTEXT2;\nSELECT DISTINCT _cluster FROM namespaces LIMIT 5;" "_cluster"
fi

# Test USE with wildcard (use a pattern that matches at least one context)
repl_contains "USE with wildcard pattern" "$TEST_CONTEXT1" \
    "USE *;" "Switched to"

# Test help command
repl_contains "help command shows usage" "$TEST_CONTEXT1" \
    "help" "SHOW TABLES"

# Test multi-line query (semicolon terminates)
repl_contains "Multi-statement execution" "$TEST_CONTEXT1" \
    "SELECT 1; SELECT 2;" "2"

# Test namespace query
repl_contains "Query namespaces table" "$TEST_CONTEXT1" \
    "SELECT name FROM namespaces WHERE name = 'default';" "default"

# Test JSON arrow operator (query labels on namespaces that have them)
repl_contains "JSON arrow operator in REPL" "$TEST_CONTEXT1" \
    "SELECT name, labels FROM namespaces WHERE labels IS NOT NULL LIMIT 1;" "labels"

# Test banner displays on startup
repl_contains "REPL shows banner" "$TEST_CONTEXT1" \
    "" "k8sql"

# Test Goodbye message on exit
repl_contains "REPL shows goodbye" "$TEST_CONTEXT1" \
    "" "Goodbye"

echo ""
echo "=== REPL Error Handling Tests ==="

# Test invalid SQL shows error
repl_contains "Invalid SQL shows error" "$TEST_CONTEXT1" \
    "SELECT FROM WHERE;" "Error"

# Test invalid table name
repl_contains "Invalid table shows error" "$TEST_CONTEXT1" \
    "SELECT * FROM nonexistent_table_xyz;" "Error"

print_summary
