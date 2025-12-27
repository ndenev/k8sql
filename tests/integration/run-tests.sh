#!/usr/bin/env bash
# Run all k8sql integration tests
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Set K8SQL binary path - convert to absolute path before changing directories
if [[ -n "${K8SQL:-}" ]]; then
    # If K8SQL is set, resolve to absolute path from current directory
    if [[ "$K8SQL" = /* ]]; then
        # Already absolute
        export K8SQL
    else
        # Convert relative to absolute
        export K8SQL="$(cd "$(dirname "$K8SQL")" && pwd)/$(basename "$K8SQL")"
    fi
else
    # Default path relative to script directory
    export K8SQL="$SCRIPT_DIR/../../bin/k8sql"
fi

cd "$SCRIPT_DIR"

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

FAILED_TESTS=()
TOTAL_SUITES=0

# Run each test script
for test_script in tests/*.sh; do
    if [[ -f "$test_script" ]]; then
        echo ""
        echo "----------------------------------------"
        echo "Running: $test_script"
        echo "----------------------------------------"

        TOTAL_SUITES=$((TOTAL_SUITES + 1))

        # Run the test script - it will exit non-zero if any tests fail
        if ! bash "$test_script"; then
            FAILED_TESTS+=("$test_script")
        fi
    fi
done

echo ""
echo "========================================"
echo "Integration Test Summary"
echo "========================================"
echo "Test suites: $TOTAL_SUITES"
echo "Failed suites: ${#FAILED_TESTS[@]}"

if [[ ${#FAILED_TESTS[@]} -gt 0 ]]; then
    echo ""
    echo "Failed test files:"
    for test in "${FAILED_TESTS[@]}"; do
        echo "  - $test"
    done
    echo "========================================"
    exit 1
fi

echo "========================================"
exit 0
