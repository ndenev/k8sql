#!/usr/bin/env bash
# Test library for k8sql integration tests

# Path to k8sql binary (relative to integration test dir)
K8SQL="${K8SQL:-../../bin/k8sql}"

# Test counters
PASS=0
FAIL=0

# Colors for output (if terminal supports it)
if [[ -t 1 ]]; then
    GREEN='\033[0;32m'
    RED='\033[0;31m'
    NC='\033[0m' # No Color
else
    GREEN=''
    RED=''
    NC=''
fi

# Run a k8sql query and capture output
run_query() {
    local context="$1"
    local query="$2"
    $K8SQL -c "$context" -q "$query" -o json 2>/dev/null
}

# Run a k8sql query with table output
run_query_table() {
    local context="$1"
    local query="$2"
    $K8SQL -c "$context" -q "$query" 2>/dev/null
}

# Assert query returns expected row count
assert_row_count() {
    local desc="$1"
    local context="$2"
    local query="$3"
    local expected="$4"

    local result
    result=$(run_query "$context" "$query")
    local count
    count=$(echo "$result" | jq 'length' 2>/dev/null || echo "error")

    if [[ "$count" == "$expected" ]]; then
        echo -e "${GREEN}✓${NC} $desc"
        PASS=$((PASS + 1))
    else
        echo -e "${RED}✗${NC} $desc (expected $expected rows, got $count)"
        FAIL=$((FAIL + 1))
    fi
}

# Assert query returns at least N rows
assert_min_row_count() {
    local desc="$1"
    local context="$2"
    local query="$3"
    local min_expected="$4"

    local result
    result=$(run_query "$context" "$query")
    local count
    count=$(echo "$result" | jq 'length' 2>/dev/null || echo "0")

    if [[ "$count" -ge "$min_expected" ]]; then
        echo -e "${GREEN}✓${NC} $desc (got $count rows)"
        PASS=$((PASS + 1))
    else
        echo -e "${RED}✗${NC} $desc (expected at least $min_expected rows, got $count)"
        FAIL=$((FAIL + 1))
    fi
}

# Assert query output contains expected value
assert_contains() {
    local desc="$1"
    local context="$2"
    local query="$3"
    local expected="$4"

    local result
    result=$(run_query "$context" "$query")

    if echo "$result" | grep -q "$expected"; then
        echo -e "${GREEN}✓${NC} $desc"
        PASS=$((PASS + 1))
    else
        echo -e "${RED}✗${NC} $desc (expected to contain: $expected)"
        FAIL=$((FAIL + 1))
    fi
}

# Assert table output contains expected value
assert_table_contains() {
    local desc="$1"
    local context="$2"
    local query="$3"
    local expected="$4"

    local result
    result=$(run_query_table "$context" "$query")

    if echo "$result" | grep -q "$expected"; then
        echo -e "${GREEN}✓${NC} $desc"
        PASS=$((PASS + 1))
    else
        echo -e "${RED}✗${NC} $desc (expected to contain: $expected)"
        FAIL=$((FAIL + 1))
    fi
}

# Assert query fails with expected error
assert_error() {
    local desc="$1"
    local context="$2"
    local query="$3"
    local expected_error="$4"

    local result
    result=$($K8SQL -c "$context" -q "$query" 2>&1)

    if echo "$result" | grep -qi "$expected_error"; then
        echo -e "${GREEN}✓${NC} $desc"
        PASS=$((PASS + 1))
    else
        echo -e "${RED}✗${NC} $desc (expected error containing: $expected_error)"
        echo "    Got: $result"
        FAIL=$((FAIL + 1))
    fi
}

# Assert query succeeds (doesn't error)
assert_success() {
    local desc="$1"
    local context="$2"
    local query="$3"

    if $K8SQL -c "$context" -q "$query" >/dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} $desc"
        PASS=$((PASS + 1))
    else
        echo -e "${RED}✗${NC} $desc (query failed)"
        FAIL=$((FAIL + 1))
    fi
}

# Print test summary and exit with appropriate code
print_summary() {
    echo ""
    echo "================================"
    echo "Tests: $((PASS + FAIL)), Passed: $PASS, Failed: $FAIL"

    if [[ $FAIL -gt 0 ]]; then
        return 1
    fi
    return 0
}

# Export counters for use in subshells
export PASS FAIL
