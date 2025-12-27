# k8sql Makefile
# Convenience targets for building, testing, and development

.PHONY: all build build-release test test-unit test-integration lint fmt clean help

# Default target
all: build test-unit

# Build debug binary
build:
	cargo build

# Build release binary and copy to bin/
build-release:
	cargo build --release
	mkdir -p bin
	cp target/release/k8sql bin/

# Run all tests (unit + integration)
test: test-unit test-integration

# Run unit tests only
test-unit:
	cargo test --verbose

# Run integration tests (requires Docker and k3d)
# This will create k3d clusters, run tests, and clean up
test-integration: build-release
	@echo "=== Running integration tests ==="
	@echo "Prerequisites: Docker and k3d must be installed"
	@command -v docker >/dev/null 2>&1 || { echo "Error: docker is not installed"; exit 1; }
	@command -v k3d >/dev/null 2>&1 || { echo "Error: k3d is not installed"; exit 1; }
	./tests/integration/setup-clusters.sh
	K8SQL=./bin/k8sql ./tests/integration/run-tests.sh || (./tests/integration/cleanup.sh && exit 1)
	./tests/integration/cleanup.sh

# Run integration tests without setup/cleanup (for debugging)
test-integration-run:
	K8SQL=./bin/k8sql ./tests/integration/run-tests.sh

# Setup integration test clusters only
test-integration-setup:
	./tests/integration/setup-clusters.sh

# Cleanup integration test clusters
test-integration-cleanup:
	./tests/integration/cleanup.sh

# Linting - check formatting and run clippy
lint:
	cargo fmt --check
	cargo clippy -- -D warnings

# Format code
fmt:
	cargo fmt

# Run clippy with auto-fix
fix:
	cargo clippy --fix --allow-dirty --allow-staged

# Clean build artifacts
clean:
	cargo clean
	rm -rf bin/

# Full clean including test clusters
clean-all: clean test-integration-cleanup

# Install the binary to ~/.cargo/bin
install: build-release
	cp target/release/k8sql ~/.cargo/bin/

# Show help
help:
	@echo "k8sql Makefile targets:"
	@echo ""
	@echo "  build              - Build debug binary"
	@echo "  build-release      - Build release binary to bin/"
	@echo "  test               - Run all tests (unit + integration)"
	@echo "  test-unit          - Run unit tests only"
	@echo "  test-integration   - Run integration tests with k3d clusters"
	@echo "  lint               - Check formatting and run clippy"
	@echo "  fmt                - Format code with rustfmt"
	@echo "  fix                - Run clippy with auto-fix"
	@echo "  clean              - Remove build artifacts"
	@echo "  clean-all          - Remove build artifacts and test clusters"
	@echo "  install            - Install binary to ~/.cargo/bin"
	@echo "  help               - Show this help"
	@echo ""
	@echo "Integration test helpers:"
	@echo "  test-integration-setup   - Setup k3d clusters only"
	@echo "  test-integration-run     - Run tests only (assumes clusters exist)"
	@echo "  test-integration-cleanup - Cleanup k3d clusters"
