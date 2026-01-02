#!/bin/sh
# Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
# SPDX-License-Identifier: BSD-3-Clause

# K8SQL Installer
# Quick install: curl -sSfL https://raw.githubusercontent.com/ndenev/k8sql/master/install.sh | sh

set -e

# Create temporary directory for downloads
TEMP_DIR=$(mktemp -d 2>/dev/null || mktemp -d -t 'k8sql-install')

# Cleanup on exit
cleanup() {
    if [ -d "$TEMP_DIR" ]; then
        rm -rf "$TEMP_DIR"
    fi
}
trap cleanup EXIT INT TERM

# Change to temp directory
cd "$TEMP_DIR"

# Detect OS
OS="$(uname -s)"
case "$OS" in
    Linux*)     OS="linux" ;;
    Darwin*)    OS="macos" ;;
    MINGW*|MSYS*|CYGWIN*)
        echo "Error: Native Windows is not supported"
        echo "Please use Windows Subsystem for Linux (WSL)"
        exit 1
        ;;
    *)
        echo "Error: Unsupported operating system: $OS"
        echo "Supported: Linux, macOS"
        echo "Windows users: use WSL"
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
BINARY_NAME="k8sql-${OS}-${ARCH}"

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
    echo "Error: Could not download checksum file"
    echo "URL: $CHECKSUM_URL"
    exit 1
fi

# Verify checksum (extract hash from "hash  filename" format)
EXPECTED_HASH=$(awk '{print $1}' k8sql.sha256)
if command -v sha256sum >/dev/null 2>&1; then
    # Linux
    ACTUAL_HASH=$(sha256sum k8sql | awk '{print $1}')
    if [ "$EXPECTED_HASH" != "$ACTUAL_HASH" ]; then
        echo "Error: Checksum verification failed"
        echo "Expected: $EXPECTED_HASH"
        echo "Got:      $ACTUAL_HASH"
        exit 1
    fi
    echo "Checksum verified successfully"
elif command -v shasum >/dev/null 2>&1; then
    # macOS
    ACTUAL_HASH=$(shasum -a 256 k8sql | awk '{print $1}')
    if [ "$EXPECTED_HASH" != "$ACTUAL_HASH" ]; then
        echo "Error: Checksum verification failed"
        echo "Expected: $EXPECTED_HASH"
        echo "Got:      $ACTUAL_HASH"
        exit 1
    fi
    echo "Checksum verified successfully"
else
    echo "Error: sha256sum or shasum not found"
    exit 1
fi

# Verify it's actually a binary (not an HTML error page)
if command -v file >/dev/null 2>&1; then
    FILE_TYPE=$(file k8sql 2>/dev/null || echo "unknown")
    if ! echo "$FILE_TYPE" | grep -q "executable\|ELF\|Mach-O"; then
        echo "Error: Downloaded file is not a valid executable"
        echo "File type: $FILE_TYPE"
        exit 1
    fi
fi

# Make executable
chmod +x k8sql

# Install - try system location first, fall back to user location
if mv k8sql /usr/local/bin/k8sql 2>/dev/null; then
    INSTALL_DIR="/usr/local/bin"
    echo "Installed to /usr/local/bin/k8sql"
else
    INSTALL_DIR="$HOME/.local/bin"
    mkdir -p "$INSTALL_DIR"
    mv k8sql "${INSTALL_DIR}/k8sql"
    echo "Installed to ${INSTALL_DIR}/k8sql"
fi

# Verify installation
if command -v k8sql >/dev/null 2>&1; then
    echo ""
    echo "✓ k8sql ${LATEST_VERSION} installed successfully!"
    echo ""
    if ! k8sql --version; then
        echo "Warning: k8sql --version failed (may be missing dependencies)"
    fi
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
