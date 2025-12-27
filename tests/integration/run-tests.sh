#!/usr/bin/env bash
# Run all k8sql integration tests
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Set K8SQL binary path
export K8SQL="${K8SQL:-../../bin/k8sql}"

# Check that k8sql binary exists
if [[ ! -x "$K8SQL" ]]; then
    echo "Error: k8sql binary not found at $K8SQL"
    echo "Build it first with: cargo build --release && mkdir -p bin && cp target/release/k8sql bin/"
    exit 1
fi

# Check that jq is available for JSON parsing
if ! command -v jq &> /dev/null; then
    echo "Error: jq is required for running tests"
    exit 1
fi

echo "========================================"
echo "Running k8sql integration tests"
echo "========================================"
echo "Binary: $K8SQL"
echo ""

TOTAL_PASS=0
TOTAL_FAIL=0
FAILED_TESTS=()

# Run each test script
for test_script in tests/*.sh; do
    if [[ -f "$test_script" ]]; then
        echo ""
        echo "----------------------------------------"
        echo "Running: $test_script"
        echo "----------------------------------------"

        # Source lib.sh fresh for each test to reset counters
        PASS=0
        FAIL=0
        source ./lib.sh

        # Run the test script
        if bash "$test_script"; then
            TOTAL_PASS=$((TOTAL_PASS + PASS))
            TOTAL_FAIL=$((TOTAL_FAIL + FAIL))
        else
            TOTAL_PASS=$((TOTAL_PASS + PASS))
            TOTAL_FAIL=$((TOTAL_FAIL + FAIL))
            FAILED_TESTS+=("$test_script")
        fi
    fi
done

echo ""
echo "========================================"
echo "Integration Test Summary"
echo "========================================"
echo "Total Tests: $((TOTAL_PASS + TOTAL_FAIL))"
echo "Passed: $TOTAL_PASS"
echo "Failed: $TOTAL_FAIL"

if [[ ${#FAILED_TESTS[@]} -gt 0 ]]; then
    echo ""
    echo "Failed test files:"
    for test in "${FAILED_TESTS[@]}"; do
        echo "  - $test"
    done
fi

echo "========================================"

if [[ $TOTAL_FAIL -gt 0 ]]; then
    exit 1
fi
exit 0
