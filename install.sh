#!/bin/sh
# Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
# SPDX-License-Identifier: BSD-3-Clause

# K8SQL Installer
# Quick install: curl -sSfL https://raw.githubusercontent.com/ndenev/k8sql/master/install.sh | sh

set -e

# Cleanup on exit
cleanup() {
    if [ -f "k8sql.tmp" ]; then
        rm -f k8sql.tmp
    fi
    if [ -f "k8sql.sha256" ]; then
        rm -f k8sql.sha256
    fi
}
trap cleanup EXIT INT TERM

# Detect OS
OS="$(uname -s)"
case "$OS" in
    Linux*)     OS="linux" ;;
    Darwin*)    OS="macos" ;;
    MINGW*|MSYS*|CYGWIN*) OS="windows" ;;
    *)
        echo "Error: Unsupported operating system: $OS"
        echo "Supported: Linux, macOS, Windows (via WSL)"
        exit 1
        ;;
esac

# Detect architecture
ARCH="$(uname -m)"
case "$ARCH" in
    x86_64|amd64)   ARCH="x86_64" ;;
    aarch64|arm64)  ARCH="aarch64" ;;
    *)
        echo "Error: Unsupported architecture: $ARCH"
        echo "Supported: x86_64, aarch64"
        exit 1
        ;;
esac

# Construct binary name
if [ "$OS" = "windows" ]; then
    BINARY_NAME="k8sql-${OS}-${ARCH}.exe"
else
    BINARY_NAME="k8sql-${OS}-${ARCH}"
fi

# Get latest release version from GitHub API
echo "Fetching latest release..."

# Try jq first (most robust), fall back to grep/sed
if command -v jq >/dev/null 2>&1; then
    LATEST_VERSION=$(curl -sSfL https://api.github.com/repos/ndenev/k8sql/releases/latest | jq -r '.tag_name')
else
    # Fallback: use GitHub redirect endpoint (more reliable than parsing JSON with grep)
    LATEST_VERSION=$(curl -sSfLI -o /dev/null -w '%{url_effective}' https://github.com/ndenev/k8sql/releases/latest | sed 's|.*/||')
fi

if [ -z "$LATEST_VERSION" ] || [ "$LATEST_VERSION" = "null" ]; then
    echo "Error: Could not determine latest version"
    echo "Please check your internet connection or try again later"
    exit 1
fi

echo "Latest version: $LATEST_VERSION"

# Download URL
DOWNLOAD_URL="https://github.com/ndenev/k8sql/releases/download/${LATEST_VERSION}/${BINARY_NAME}"

echo "Downloading ${BINARY_NAME}..."
if ! curl -sSfL "$DOWNLOAD_URL" -o k8sql; then
    echo "Error: Failed to download binary"
    echo "URL: $DOWNLOAD_URL"
    exit 1
fi

# Download and verify checksum
echo "Verifying checksum..."
CHECKSUM_URL="${DOWNLOAD_URL}.sha256"
if ! curl -sSfL "$CHECKSUM_URL" -o k8sql.sha256; then
    echo "Warning: Could not download checksum file"
    echo "Proceeding without verification (not recommended)"
    echo "URL: $CHECKSUM_URL"
else
    # Verify checksum
    if command -v sha256sum >/dev/null 2>&1; then
        # Linux
        echo "$(cat k8sql.sha256)  k8sql" | sha256sum -c - || {
            echo "Error: Checksum verification failed"
            exit 1
        }
    elif command -v shasum >/dev/null 2>&1; then
        # macOS
        echo "$(cat k8sql.sha256)  k8sql" | shasum -a 256 -c - || {
            echo "Error: Checksum verification failed"
            exit 1
        }
    else
        echo "Warning: sha256sum/shasum not found, cannot verify checksum"
    fi
fi

# Verify it's actually a binary (not an HTML error page)
if ! file k8sql 2>/dev/null | grep -q "executable\|ELF\|Mach-O"; then
    if command -v file >/dev/null 2>&1; then
        echo "Error: Downloaded file is not a valid executable"
        echo "File type: $(file k8sql)"
        exit 1
    fi
    # If 'file' command not available, skip verification
fi

# Make executable
chmod +x k8sql

# Determine install location
if [ -w /usr/local/bin ]; then
    INSTALL_DIR="/usr/local/bin"
else
    INSTALL_DIR="$HOME/.local/bin"
    mkdir -p "$INSTALL_DIR"
fi

# Install
echo "Installing to ${INSTALL_DIR}/k8sql..."
mv k8sql "${INSTALL_DIR}/k8sql"

# Verify installation
if command -v k8sql >/dev/null 2>&1; then
    echo ""
    echo "✓ k8sql ${LATEST_VERSION} installed successfully!"
    echo ""
    k8sql --version
    echo ""
    echo "Run 'k8sql --help' to get started."
else
    echo ""
    echo "✓ k8sql ${LATEST_VERSION} installed to ${INSTALL_DIR}/k8sql"
    echo ""
    echo "Note: ${INSTALL_DIR} is not in your PATH."
    echo "Add this to your shell profile (~/.bashrc, ~/.zshrc, etc.):"
    echo "    export PATH=\"\$PATH:${INSTALL_DIR}\""
    echo ""
    echo "Then run 'k8sql --help' to get started."
fi
