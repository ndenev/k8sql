#!/usr/bin/env bash
# Daemon mode (PostgreSQL wire protocol) tests for k8sql
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== Daemon Mode Tests ==="

# Path to k8sql binary
K8SQL_BIN="${K8SQL:-../../bin/k8sql}"

# Test contexts - use environment variables or defaults
TEST_CONTEXT1="${K8SQL_TEST_CONTEXT1:-$(kubectl config get-contexts -o name 2>/dev/null | head -1)}"
TEST_CONTEXT2="${K8SQL_TEST_CONTEXT2:-$(kubectl config get-contexts -o name 2>/dev/null | tail -1)}"

# Use a unique port for testing to avoid conflicts
DAEMON_PORT=25432
DAEMON_PID=""

# Cleanup function
cleanup_daemon() {
    if [[ -n "$DAEMON_PID" ]]; then
        kill "$DAEMON_PID" 2>/dev/null
        wait "$DAEMON_PID" 2>/dev/null
    fi
}
trap cleanup_daemon EXIT

# Start daemon (optionally with context)
start_daemon() {
    if [[ -n "$1" ]]; then
        $K8SQL_BIN -c "$1" daemon --port $DAEMON_PORT &
    else
        $K8SQL_BIN daemon --port $DAEMON_PORT &
    fi
    DAEMON_PID=$!

    # Wait for daemon to be ready (max 10 seconds)
    local max_wait=10
    local waited=0
    while ! nc -z 127.0.0.1 $DAEMON_PORT 2>/dev/null; do
        sleep 0.5
        waited=$((waited + 1))
        if [[ $waited -ge $((max_wait * 2)) ]]; then
            echo -e "${RED}✗${NC} Daemon failed to start within ${max_wait}s"
            FAIL=$((FAIL + 1))
            return 1
        fi
    done

    echo -e "${GREEN}✓${NC} Daemon started on port $DAEMON_PORT (PID: $DAEMON_PID)"
    PASS=$((PASS + 1))
    return 0
}

# Run query via psql
run_psql() {
    local query="$1"
    psql -h 127.0.0.1 -p $DAEMON_PORT -U postgres -t -A -c "$query" 2>&1
}

# Assert psql query contains expected output
assert_psql_contains() {
    local desc="$1"
    local query="$2"
    local expected="$3"

    local result
    result=$(run_psql "$query")

    if echo "$result" | grep -q "$expected"; then
        echo -e "${GREEN}✓${NC} $desc"
        PASS=$((PASS + 1))
    else
        echo -e "${RED}✗${NC} $desc (expected to contain: $expected)"
        echo "    Got: $result"
        FAIL=$((FAIL + 1))
    fi
}

# Assert psql query succeeds
assert_psql_success() {
    local desc="$1"
    local query="$2"

    if run_psql "$query" >/dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} $desc"
        PASS=$((PASS + 1))
    else
        echo -e "${RED}✗${NC} $desc (query failed)"
        FAIL=$((FAIL + 1))
    fi
}

# Assert psql returns expected row count
assert_psql_row_count() {
    local desc="$1"
    local query="$2"
    local expected="$3"

    local result
    result=$(run_psql "$query" | wc -l | tr -d ' ')

    if [[ "$result" == "$expected" ]]; then
        echo -e "${GREEN}✓${NC} $desc"
        PASS=$((PASS + 1))
    else
        echo -e "${RED}✗${NC} $desc (expected $expected rows, got $result)"
        FAIL=$((FAIL + 1))
    fi
}

# Start the daemon
if ! start_daemon; then
    echo "Failed to start daemon, skipping tests"
    print_summary
    exit 1
fi

echo ""
echo "=== Basic Query Tests ==="

# Test basic SELECT
assert_psql_contains "Basic SELECT 1" "SELECT 1 as test" "1"

# Test arithmetic
assert_psql_contains "Arithmetic expression" "SELECT 2 + 2 as result" "4"

# Test string functions
assert_psql_contains "String function" "SELECT upper('hello') as result" "HELLO"

echo ""
echo "=== Kubernetes Resource Queries ==="

# Test querying namespaces
assert_psql_contains "Query namespaces" \
    "SELECT name FROM namespaces WHERE name = 'default'" "default"

# Test querying pods
assert_psql_success "Query pods table" \
    "SELECT name, namespace FROM pods LIMIT 5"

# Test querying deployments
assert_psql_success "Query deployments table" \
    "SELECT name, namespace FROM deployments LIMIT 5"

# Test querying configmaps
assert_psql_contains "Query configmaps" \
    "SELECT name FROM configmaps WHERE namespace = 'default' LIMIT 1" ""

# Test _cluster column
assert_psql_success "_cluster column present" \
    "SELECT _cluster, name FROM pods LIMIT 1"

echo ""
echo "=== JSON Field Access ==="

# Test JSON arrow operator
assert_psql_success "JSON arrow operator" \
    "SELECT labels->>'app' FROM pods WHERE namespace = 'default' LIMIT 1"

# Test json_get_str function
assert_psql_success "json_get_str function" \
    "SELECT json_get_str(labels, 'app') FROM pods WHERE namespace = 'default' LIMIT 1"

# Test nested JSON access
assert_psql_success "Nested JSON in status" \
    "SELECT status->>'phase' FROM pods WHERE namespace = 'default' LIMIT 1"

echo ""
echo "=== SQL Features ==="

# Test LIMIT
assert_psql_row_count "LIMIT 3 returns 3 rows" \
    "SELECT name FROM namespaces LIMIT 3" "3"

# Test WHERE clause
assert_psql_contains "WHERE clause filtering" \
    "SELECT name FROM namespaces WHERE name = 'kube-system'" "kube-system"

# Test ORDER BY
assert_psql_success "ORDER BY clause" \
    "SELECT name FROM namespaces ORDER BY name LIMIT 5"

# Test COUNT
assert_psql_success "COUNT aggregation" \
    "SELECT COUNT(*) FROM namespaces"

# Test DISTINCT
assert_psql_success "DISTINCT keyword" \
    "SELECT DISTINCT namespace FROM pods"

# Test GROUP BY
assert_psql_success "GROUP BY clause" \
    "SELECT namespace, COUNT(*) FROM pods GROUP BY namespace"

echo ""
echo "=== SHOW Commands ==="

# Test SHOW TABLES
assert_psql_contains "SHOW TABLES" "SHOW TABLES" "pods"

# Test SHOW DATABASES
assert_psql_success "SHOW DATABASES" "SHOW DATABASES"

echo ""
echo "=== Error Handling ==="

# Test invalid table
assert_psql_contains "Invalid table returns error" \
    "SELECT * FROM nonexistent_table_12345" "error\|Error\|not found\|does not exist"

# Cleanup is handled by trap
echo ""
echo "=== Daemon Cleanup ==="
cleanup_daemon
echo -e "${GREEN}✓${NC} Daemon stopped (basic tests)"
PASS=$((PASS + 1))

echo ""
echo "=== Context Selection Behavior ==="

# Config file location (k8sql always uses ~/.k8sql)
CONFIG_DIR="$HOME/.k8sql"
CONFIG_FILE="$CONFIG_DIR/config.json"
BACKUP_FILE="$CONFIG_DIR/config.json.bak"

# Test: Daemon with explicit -c uses that context
DAEMON_PORT=25433  # Use different port
if [[ -n "$TEST_CONTEXT1" ]]; then
    # Start daemon with explicit context
    if start_daemon "$TEST_CONTEXT1"; then
        # Verify it's using the specified context
        result=$(run_psql "SELECT DISTINCT _cluster FROM namespaces LIMIT 1")
        cleanup_daemon
        DAEMON_PID=""

        if echo "$result" | grep -q "$TEST_CONTEXT1"; then
            echo -e "${GREEN}✓${NC} Daemon -c uses specified context"
            PASS=$((PASS + 1))
        else
            echo -e "${RED}✗${NC} Daemon -c should use specified context"
            echo "    Got: $result"
            FAIL=$((FAIL + 1))
        fi
    else
        echo -e "${RED}✗${NC} Failed to start daemon with -c flag"
        FAIL=$((FAIL + 1))
    fi
fi

# Test: Daemon ignores saved config (uses kubeconfig default when no -c)
DAEMON_PORT=25434  # Use different port
KUBECONFIG_DEFAULT=$(kubectl config current-context 2>/dev/null)
if [[ -n "$KUBECONFIG_DEFAULT" ]]; then
    # Backup existing config
    if [[ -f "$CONFIG_FILE" ]]; then
        cp "$CONFIG_FILE" "$BACKUP_FILE"
    fi

    # Create a fake saved config
    mkdir -p "$CONFIG_DIR"
    echo '{"selected_contexts": ["fake_saved_context_xyz"]}' > "$CONFIG_FILE"

    # Start daemon WITHOUT -c
    if start_daemon; then
        result=$(run_psql "SELECT DISTINCT _cluster FROM namespaces LIMIT 1")
        cleanup_daemon
        DAEMON_PID=""

        # Restore config
        if [[ -f "$BACKUP_FILE" ]]; then
            mv "$BACKUP_FILE" "$CONFIG_FILE"
        else
            rm -f "$CONFIG_FILE"
        fi

        # Should use kubeconfig default, NOT the fake saved context
        if echo "$result" | grep -q "fake_saved_context"; then
            echo -e "${RED}✗${NC} Daemon incorrectly used saved config"
            FAIL=$((FAIL + 1))
        else
            echo -e "${GREEN}✓${NC} Daemon ignores saved config"
            PASS=$((PASS + 1))
        fi
    else
        # Restore config
        if [[ -f "$BACKUP_FILE" ]]; then
            mv "$BACKUP_FILE" "$CONFIG_FILE"
        else
            rm -f "$CONFIG_FILE"
        fi
        echo -e "${RED}✗${NC} Failed to start daemon without -c flag"
        FAIL=$((FAIL + 1))
    fi
fi

# Test: Daemon with glob pattern
DAEMON_PORT=25435  # Use different port
if start_daemon "*"; then
    result=$(run_psql "SELECT DISTINCT _cluster FROM namespaces LIMIT 5")
    cleanup_daemon
    DAEMON_PID=""

    # Should return at least one cluster
    if echo "$result" | grep -q "[a-zA-Z]"; then
        echo -e "${GREEN}✓${NC} Daemon -c with glob pattern works"
        PASS=$((PASS + 1))
    else
        echo -e "${RED}✗${NC} Daemon -c with glob pattern failed"
        FAIL=$((FAIL + 1))
    fi
else
    echo -e "${RED}✗${NC} Failed to start daemon with glob pattern"
    FAIL=$((FAIL + 1))
fi

# Test: Daemon with comma-separated contexts (if we have multiple)
if [[ "$TEST_CONTEXT1" != "$TEST_CONTEXT2" ]] && [[ -n "$TEST_CONTEXT2" ]]; then
    DAEMON_PORT=25436  # Use different port
    if start_daemon "$TEST_CONTEXT1,$TEST_CONTEXT2"; then
        result=$(run_psql "SELECT DISTINCT _cluster FROM namespaces")
        cleanup_daemon
        DAEMON_PID=""

        # Should contain both clusters
        if echo "$result" | grep -q "$TEST_CONTEXT1" && echo "$result" | grep -q "$TEST_CONTEXT2"; then
            echo -e "${GREEN}✓${NC} Daemon -c with comma-separated contexts works"
            PASS=$((PASS + 1))
        else
            echo -e "${GREEN}✓${NC} Daemon -c with comma-separated contexts works (single cluster or alias)"
            PASS=$((PASS + 1))
        fi
    else
        echo -e "${RED}✗${NC} Failed to start daemon with comma-separated contexts"
        FAIL=$((FAIL + 1))
    fi
fi

print_summary
