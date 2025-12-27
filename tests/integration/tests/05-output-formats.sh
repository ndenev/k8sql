#!/usr/bin/env bash
# Output format tests for k8sql
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== Output Format Tests ==="

K8SQL="${K8SQL:-../../bin/k8sql}"

# JSON output - valid JSON array
result=$($K8SQL -c k3d-k8sql-test-1 -q "SELECT name FROM configmaps WHERE namespace = 'default' LIMIT 1" -o json 2>/dev/null)
if echo "$result" | jq -e '.[0].name' > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} JSON output is valid"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} JSON output invalid: $result"
    FAIL=$((FAIL + 1))
fi

# JSON output - array structure
result=$($K8SQL -c k3d-k8sql-test-1 -q "SELECT name FROM namespaces LIMIT 3" -o json 2>/dev/null)
if echo "$result" | jq -e 'type == "array"' > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} JSON output is an array"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} JSON output is not an array"
    FAIL=$((FAIL + 1))
fi

# CSV output - has header row
result=$($K8SQL -c k3d-k8sql-test-1 -q "SELECT name FROM configmaps WHERE namespace = 'default' LIMIT 1" -o csv 2>/dev/null)
if echo "$result" | head -1 | grep -q "name"; then
    echo -e "${GREEN}✓${NC} CSV output has header"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} CSV output missing header"
    FAIL=$((FAIL + 1))
fi

# CSV output - multiple columns
result=$($K8SQL -c k3d-k8sql-test-1 -q "SELECT name, namespace FROM pods LIMIT 1" -o csv 2>/dev/null)
if echo "$result" | head -1 | grep -q "name" && echo "$result" | head -1 | grep -q "namespace"; then
    echo -e "${GREEN}✓${NC} CSV output has multiple columns"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} CSV output missing columns"
    FAIL=$((FAIL + 1))
fi

# YAML output - valid YAML structure
result=$($K8SQL -c k3d-k8sql-test-1 -q "SELECT name FROM configmaps WHERE namespace = 'default' LIMIT 1" -o yaml 2>/dev/null)
if echo "$result" | grep -q "name:"; then
    echo -e "${GREEN}✓${NC} YAML output is valid"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} YAML output invalid"
    FAIL=$((FAIL + 1))
fi

# Table output (default) - has borders/structure
result=$($K8SQL -c k3d-k8sql-test-1 -q "SELECT name FROM namespaces LIMIT 2" 2>/dev/null)
if echo "$result" | grep -qE '[-+|]|name'; then
    echo -e "${GREEN}✓${NC} Table output has structure"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} Table output missing structure"
    FAIL=$((FAIL + 1))
fi

# Empty result handling - JSON
result=$($K8SQL -c k3d-k8sql-test-1 -q "SELECT name FROM pods WHERE namespace = 'nonexistent'" -o json 2>/dev/null)
if echo "$result" | jq -e 'length == 0' > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Empty JSON result is empty array"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} Empty JSON result not handled correctly"
    FAIL=$((FAIL + 1))
fi

# Multiple columns in JSON
result=$($K8SQL -c k3d-k8sql-test-1 -q "SELECT name, namespace, _cluster FROM pods WHERE namespace = 'default' LIMIT 1" -o json 2>/dev/null)
if echo "$result" | jq -e '.[0] | has("name") and has("namespace") and has("_cluster")' > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} JSON has all selected columns"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} JSON missing columns"
    FAIL=$((FAIL + 1))
fi

print_summary
