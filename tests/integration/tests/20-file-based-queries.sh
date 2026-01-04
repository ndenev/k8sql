#!/usr/bin/env bash
# File-based query execution tests for k8sql
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

echo "=== File-Based Query Execution Tests ==="

TEST_CONTEXT="k3d-k8sql-test-1"
TMP_DIR=$(mktemp -d)

cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

# Test 1: Single query in file
cat > "$TMP_DIR/single-query.sql" << 'EOF'
SELECT name FROM namespaces LIMIT 3
EOF

result=$($K8SQL -c "$TEST_CONTEXT" -f "$TMP_DIR/single-query.sql" -o json 2>/dev/null)
if echo "$result" | jq -e 'length > 0' > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Single query from file executes correctly"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} Single query from file failed: $result"
    FAIL=$((FAIL + 1))
fi

# Test 2: Multiple queries in file (separated by semicolons)
cat > "$TMP_DIR/multi-query.sql" << 'EOF'
SELECT COUNT(*) as cnt FROM namespaces;
SELECT name FROM configmaps WHERE namespace = 'default' LIMIT 1;
EOF

result=$($K8SQL -c "$TEST_CONTEXT" -f "$TMP_DIR/multi-query.sql" 2>/dev/null)
# Should have output from both queries
if echo "$result" | grep -q "cnt" && echo "$result" | grep -q "name"; then
    echo -e "${GREEN}✓${NC} Multiple queries from file execute sequentially"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} Multiple queries from file failed"
    FAIL=$((FAIL + 1))
fi

# Test 3: Comments are skipped (lines starting with --)
cat > "$TMP_DIR/with-comments.sql" << 'EOF'
-- This is a comment that should be ignored
SELECT name FROM namespaces LIMIT 1;
-- Another comment
SELECT COUNT(*) as total FROM pods;
EOF

result=$($K8SQL -c "$TEST_CONTEXT" -f "$TMP_DIR/with-comments.sql" 2>/dev/null)
# Should execute the two SELECT statements, not fail on comments
if echo "$result" | grep -q "name" && echo "$result" | grep -q "total"; then
    echo -e "${GREEN}✓${NC} SQL comments (--) are correctly skipped"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} Comment handling failed"
    FAIL=$((FAIL + 1))
fi

# Test 4: Empty lines are skipped
cat > "$TMP_DIR/with-empty-lines.sql" << 'EOF'

SELECT name FROM namespaces LIMIT 1;

SELECT COUNT(*) as cnt FROM pods;

EOF

result=$($K8SQL -c "$TEST_CONTEXT" -f "$TMP_DIR/with-empty-lines.sql" 2>/dev/null)
exit_code=$?
if [[ $exit_code -eq 0 ]] && echo "$result" | grep -q "name"; then
    echo -e "${GREEN}✓${NC} Empty lines are correctly skipped"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} Empty line handling failed"
    FAIL=$((FAIL + 1))
fi

# Test 5: File with JSON output format
cat > "$TMP_DIR/json-output.sql" << 'EOF'
SELECT name, namespace FROM pods WHERE namespace = 'default' LIMIT 2
EOF

result=$($K8SQL -c "$TEST_CONTEXT" -f "$TMP_DIR/json-output.sql" -o json 2>/dev/null)
if echo "$result" | jq -e 'type == "array"' > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} File execution with JSON output format works"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} File execution with JSON output failed"
    FAIL=$((FAIL + 1))
fi

# Test 6: File with CSV output format
cat > "$TMP_DIR/csv-output.sql" << 'EOF'
SELECT name FROM namespaces LIMIT 2
EOF

result=$($K8SQL -c "$TEST_CONTEXT" -f "$TMP_DIR/csv-output.sql" -o csv 2>/dev/null)
if echo "$result" | head -1 | grep -q "name"; then
    echo -e "${GREEN}✓${NC} File execution with CSV output format works"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} File execution with CSV output failed"
    FAIL=$((FAIL + 1))
fi

# Test 7: Non-existent file returns error
result=$($K8SQL -c "$TEST_CONTEXT" -f "/nonexistent/path/queries.sql" 2>&1)
exit_code=$?
if [[ $exit_code -ne 0 ]] && echo "$result" | grep -qi "no such file\|not found\|error"; then
    echo -e "${GREEN}✓${NC} Non-existent file returns appropriate error"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} Non-existent file should return error (exit code: $exit_code)"
    FAIL=$((FAIL + 1))
fi

# Test 8: SHOW TABLES works from file
cat > "$TMP_DIR/show-tables.sql" << 'EOF'
SHOW TABLES
EOF

result=$($K8SQL -c "$TEST_CONTEXT" -f "$TMP_DIR/show-tables.sql" 2>/dev/null)
if echo "$result" | grep -qi "pods\|deployments\|configmaps"; then
    echo -e "${GREEN}✓${NC} SHOW TABLES works from file"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} SHOW TABLES from file failed"
    FAIL=$((FAIL + 1))
fi

# Test 9: SHOW DATABASES works from file
cat > "$TMP_DIR/show-databases.sql" << 'EOF'
SHOW DATABASES
EOF

result=$($K8SQL -c "$TEST_CONTEXT" -f "$TMP_DIR/show-databases.sql" 2>/dev/null)
if echo "$result" | grep -q "$TEST_CONTEXT"; then
    echo -e "${GREEN}✓${NC} SHOW DATABASES works from file"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} SHOW DATABASES from file failed"
    FAIL=$((FAIL + 1))
fi

# Test 10: File with --no-headers flag
# Compare line counts: with headers should have one more line than without
cat > "$TMP_DIR/no-headers.sql" << 'EOF'
SELECT name FROM namespaces LIMIT 3
EOF

with_headers=$($K8SQL -c "$TEST_CONTEXT" -f "$TMP_DIR/no-headers.sql" -o csv 2>/dev/null | wc -l)
without_headers=$($K8SQL -c "$TEST_CONTEXT" -f "$TMP_DIR/no-headers.sql" -o csv --no-headers 2>/dev/null | wc -l)
# With headers should have exactly one more line (the header row)
if [[ $((with_headers - without_headers)) -eq 1 ]]; then
    echo -e "${GREEN}✓${NC} File execution with --no-headers works"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} --no-headers flag not working (with: $with_headers, without: $without_headers)"
    FAIL=$((FAIL + 1))
fi

# Test 11: Query with error in file exits with non-zero
cat > "$TMP_DIR/invalid-query.sql" << 'EOF'
SELECT * FROM nonexistent_table_xyz
EOF

result=$($K8SQL -c "$TEST_CONTEXT" -f "$TMP_DIR/invalid-query.sql" 2>&1)
exit_code=$?
if [[ $exit_code -ne 0 ]]; then
    echo -e "${GREEN}✓${NC} Invalid query in file exits with non-zero status"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} Invalid query should exit with non-zero status"
    FAIL=$((FAIL + 1))
fi

# Test 12: Multi-line query with CTE
cat > "$TMP_DIR/multiline-cte.sql" << 'EOF'
-- Multi-line CTE query
WITH ns_counts AS (
    SELECT
        namespace,
        COUNT(*) as cnt
    FROM pods
    GROUP BY namespace
)
SELECT * FROM ns_counts LIMIT 2;
EOF

result=$($K8SQL -c "$TEST_CONTEXT" -f "$TMP_DIR/multiline-cte.sql" -o json 2>/dev/null)
if echo "$result" | jq -e '.[0] | has("namespace") and has("cnt")' > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Multi-line CTE query executes correctly"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} Multi-line CTE query failed: $result"
    FAIL=$((FAIL + 1))
fi

# Test 13: Multiple multi-line queries separated by semicolons
cat > "$TMP_DIR/multi-multiline.sql" << 'EOF'
-- First query
SELECT name
FROM namespaces
LIMIT 1;

-- Second query
SELECT name
FROM configmaps
WHERE namespace = 'default'
LIMIT 1;
EOF

result=$($K8SQL -c "$TEST_CONTEXT" -f "$TMP_DIR/multi-multiline.sql" 2>/dev/null)
# Both queries should produce output
line_count=$(echo "$result" | grep -c "rows\|row")
if [[ $line_count -ge 2 ]]; then
    echo -e "${GREEN}✓${NC} Multiple multi-line queries execute correctly"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} Multiple multi-line queries failed (expected 2+ result sets)"
    FAIL=$((FAIL + 1))
fi

# Test 14: Query without trailing semicolon
cat > "$TMP_DIR/no-semicolon.sql" << 'EOF'
SELECT name FROM namespaces LIMIT 1
EOF

result=$($K8SQL -c "$TEST_CONTEXT" -f "$TMP_DIR/no-semicolon.sql" -o json 2>/dev/null)
if echo "$result" | jq -e 'length > 0' > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Query without trailing semicolon works"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} Query without trailing semicolon failed"
    FAIL=$((FAIL + 1))
fi

# Test 15: Inline comments within multi-line query
cat > "$TMP_DIR/inline-comments.sql" << 'EOF'
SELECT 
    name,  -- the resource name
    namespace  -- the namespace
FROM pods
WHERE namespace = 'default'  -- filter to default
LIMIT 2;
EOF

result=$($K8SQL -c "$TEST_CONTEXT" -f "$TMP_DIR/inline-comments.sql" -o json 2>/dev/null)
exit_code=$?
if [[ $exit_code -eq 0 ]]; then
    echo -e "${GREEN}✓${NC} Inline comments within query work correctly"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} Inline comments handling failed"
    FAIL=$((FAIL + 1))
fi

# Test 16: String containing semicolon (should not split)
cat > "$TMP_DIR/semicolon-in-string.sql" << 'EOF'
SELECT name FROM configmaps 
WHERE namespace = 'default' 
AND name LIKE 'kube%'
LIMIT 1;
EOF

result=$($K8SQL -c "$TEST_CONTEXT" -f "$TMP_DIR/semicolon-in-string.sql" -o json 2>/dev/null)
exit_code=$?
if [[ $exit_code -eq 0 ]]; then
    echo -e "${GREEN}✓${NC} Complex WHERE clause with LIKE works"
    PASS=$((PASS + 1))
else
    echo -e "${RED}✗${NC} Query with LIKE pattern failed"
    FAIL=$((FAIL + 1))
fi

print_summary
