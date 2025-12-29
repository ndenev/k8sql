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

echo ""
echo "=== Context Selection Behavior ==="

# Config file location (k8sql always uses ~/.k8sql)
CONFIG_DIR="$HOME/.k8sql"
CONFIG_FILE="$CONFIG_DIR/config.json"
BACKUP_FILE="$CONFIG_DIR/config.json.bak"

# Helper to run REPL without -c flag
run_repl_no_context() {
    local commands="$*"
    echo -e "$commands\n\\q" | $K8SQL_BIN 2>&1
}

# Test: --context is a temporary override (doesn't persist)
# First, clear any existing saved config
if [[ -f "$CONFIG_FILE" ]]; then
    cp "$CONFIG_FILE" "$BACKUP_FILE"
fi
rm -f "$CONFIG_FILE"

# Run REPL with explicit -c flag
run_repl "$TEST_CONTEXT1" "SELECT 1;" >/dev/null 2>&1

# Check if config was created (it shouldn't be, -c doesn't persist)
if [[ ! -f "$CONFIG_FILE" ]] || ! grep -q "$TEST_CONTEXT1" "$CONFIG_FILE" 2>/dev/null; then
    echo -e "${GREEN}✓${NC} --context flag does not persist to config"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} --context flag incorrectly persisted to config"
    FAIL=$((FAIL + 1))
fi

# Restore original config
if [[ -f "$BACKUP_FILE" ]]; then
    mv "$BACKUP_FILE" "$CONFIG_FILE"
fi

# Test: USE command DOES persist (only in REPL mode)
if [[ "$TEST_CONTEXT1" != "$TEST_CONTEXT2" ]]; then
    # Backup existing config
    if [[ -f "$CONFIG_FILE" ]]; then
        cp "$CONFIG_FILE" "$BACKUP_FILE"
    fi
    rm -f "$CONFIG_FILE"

    # Run REPL and USE a context
    run_repl "$TEST_CONTEXT1" "USE $TEST_CONTEXT2;" >/dev/null 2>&1

    # Check if config was created with the USE context
    if [[ -f "$CONFIG_FILE" ]] && grep -q "$TEST_CONTEXT2" "$CONFIG_FILE" 2>/dev/null; then
        echo -e "${GREEN}✓${NC} USE command persists to config"
        PASS=$((PASS + 1))
    else
        echo -e "${RED}✗${NC} USE command should persist to config"
        FAIL=$((FAIL + 1))
    fi

    # Restore original config
    if [[ -f "$BACKUP_FILE" ]]; then
        mv "$BACKUP_FILE" "$CONFIG_FILE"
    else
        rm -f "$CONFIG_FILE"
    fi
fi

# Test: Without --context, saved config is restored
if [[ -f "$CONFIG_FILE" ]]; then
    cp "$CONFIG_FILE" "$BACKUP_FILE"
fi

# Create a saved config with our test context
mkdir -p "$CONFIG_DIR"
echo "{\"selected_contexts\": [\"$TEST_CONTEXT1\"]}" > "$CONFIG_FILE"

# Run REPL without -c and check it uses saved config
output=$(run_repl_no_context "SELECT DISTINCT _cluster FROM namespaces LIMIT 1;")

# Restore original config
if [[ -f "$BACKUP_FILE" ]]; then
    mv "$BACKUP_FILE" "$CONFIG_FILE"
else
    rm -f "$CONFIG_FILE"
fi

if echo "$output" | grep -q "$TEST_CONTEXT1"; then
    echo -e "${GREEN}✓${NC} REPL without -c restores saved config"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} REPL without -c should restore saved config"
    echo "    Output: $(echo "$output" | head -5)"
    FAIL=$((FAIL + 1))
fi

# Test: --context overrides saved config temporarily
if [[ "$TEST_CONTEXT1" != "$TEST_CONTEXT2" ]]; then
    # Backup and create saved config
    if [[ -f "$CONFIG_FILE" ]]; then
        cp "$CONFIG_FILE" "$BACKUP_FILE"
    fi
    mkdir -p "$CONFIG_DIR"
    echo "{\"selected_contexts\": [\"$TEST_CONTEXT2\"]}" > "$CONFIG_FILE"

    # Run with explicit -c that differs from saved config
    output=$(run_repl "$TEST_CONTEXT1" "SELECT DISTINCT _cluster FROM namespaces LIMIT 1;")

    # Restore
    if [[ -f "$BACKUP_FILE" ]]; then
        mv "$BACKUP_FILE" "$CONFIG_FILE"
    else
        rm -f "$CONFIG_FILE"
    fi

    if echo "$output" | grep -q "$TEST_CONTEXT1"; then
        echo -e "${GREEN}✓${NC} --context overrides saved config"
        PASS=$((PASS + 1))
    else
        echo -e "${RED}✗${NC} --context should override saved config"
        FAIL=$((FAIL + 1))
    fi
fi

print_summary
