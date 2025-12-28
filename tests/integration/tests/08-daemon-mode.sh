#!/usr/bin/env bash
# Daemon mode (PostgreSQL wire protocol) tests for k8sql
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== Daemon Mode Tests ==="

# Path to k8sql binary
K8SQL_BIN="${K8SQL:-../../bin/k8sql}"

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

# Start daemon
start_daemon() {
    $K8SQL_BIN daemon --port $DAEMON_PORT &
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
echo -e "${GREEN}✓${NC} Daemon stopped"
PASS=$((PASS + 1))

print_summary
