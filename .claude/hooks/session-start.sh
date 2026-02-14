#!/bin/bash
set -euo pipefail

# Only run in remote Claude Code environments
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

# Install .NET SDK if not already present
if ! command -v dotnet &> /dev/null; then
  echo "Installing .NET SDK..."

  # Try apt-get first
  if apt-get update -qq 2>/dev/null && apt-get install -y -qq dotnet-sdk-9.0 2>/dev/null; then
    echo ".NET SDK installed via apt"
  else
    echo "Apt installation failed, using dotnet-install script"
    cd /tmp
    curl -fsSL https://dot.net/v1/dotnet-install.sh -o dotnet-install.sh
    chmod +x dotnet-install.sh
    ./dotnet-install.sh --version 9.0 --install-dir /usr/local/dotnet
    export PATH="/usr/local/dotnet:$PATH"
    echo "export PATH=\"/usr/local/dotnet:\$PATH\"" >> "${CLAUDE_ENV_FILE:-/dev/null}"
  fi
fi

# Verify dotnet is available
if ! command -v dotnet &> /dev/null; then
  echo "Failed to install .NET SDK"
  exit 1
fi

echo "Restoring .NET dependencies..."
cd "${CLAUDE_PROJECT_DIR:-.}"
dotnet restore

echo "Setup complete!"
