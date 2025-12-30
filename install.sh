#!/bin/sh
# Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
# SPDX-License-Identifier: BSD-3-Clause

# K8SQL Installer
# Quick install: curl -sSfL https://raw.githubusercontent.com/ndenev/k8sql/master/install.sh | sh

set -e

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
LATEST_VERSION=$(curl -sSfL https://api.github.com/repos/ndenev/k8sql/releases/latest | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')

if [ -z "$LATEST_VERSION" ]; then
    echo "Error: Could not determine latest version"
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

# Make executable
chmod +x k8sql

# Determine install location
if [ -w /usr/local/bin ]; then
    INSTALL_DIR="/usr/local/bin"
elif [ -d "$HOME/.local/bin" ]; then
    INSTALL_DIR="$HOME/.local/bin"
    mkdir -p "$INSTALL_DIR"
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
