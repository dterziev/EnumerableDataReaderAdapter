#!/bin/bash
# Install .NET 10 SDK and essential build tools for development
# This script is intended to be run as a Claude Code Web startup hook

set -euo pipefail

# Only run in remote Claude Code environments
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

# Install essential build tools if missing
if ! command -v tail &> /dev/null || ! command -v head &> /dev/null; then
    echo "Installing essential build tools (coreutils)..."
    apt-get update -qq && apt-get install -y -qq coreutils > /dev/null 2>&1 || true
fi

echo "Downloading .NET install script..."

# Download and run the official Microsoft install script
curl -sSL https://dot.net/v1/dotnet-install.sh -o /tmp/dotnet-install.sh
chmod +x /tmp/dotnet-install.sh

INSTALL_DIR="$HOME/.dotnet"

echo "Installing additional .NET runtime frameworks..."
/tmp/dotnet-install.sh --channel 10.0 --install-dir "$INSTALL_DIR"

echo "Installing additional .NET runtime frameworks..."
/tmp/dotnet-install.sh --channel 6.0 --runtime dotnet --install-dir "$INSTALL_DIR"
/tmp/dotnet-install.sh --channel 7.0 --runtime dotnet --install-dir "$INSTALL_DIR"
/tmp/dotnet-install.sh --channel 8.0 --runtime dotnet --install-dir "$INSTALL_DIR"
/tmp/dotnet-install.sh --channel 9.0 --runtime dotnet --install-dir "$INSTALL_DIR"

# Clean up
rm -f /tmp/dotnet-install.sh

# Add to PATH for current session
export DOTNET_ROOT="$INSTALL_DIR"
export PATH="$PATH:$INSTALL_DIR:$INSTALL_DIR/tools"

# Add to PATH in the CLAUDE_ENV_FILE
echo "export DOTNET_ROOT=\"$INSTALL_DIR\"" >> "${CLAUDE_ENV_FILE:-/dev/null}"
echo "export PATH=\"\$PATH:$INSTALL_DIR:$INSTALL_DIR/tools\"" >> "${CLAUDE_ENV_FILE:-/dev/null}"

# Verify installation
echo ".NET SDK installed successfully:"
dotnet --version

# Configure NuGet to work with Claude Code proxy
# .NET's HttpClient doesn't properly handle proxy credentials from environment variables,
# so we configure NuGet to use localhost:8888 which will be handled by a proxy forwarder
echo "Configuring NuGet proxy settings..."
NUGET_CONFIG_DIR="$HOME/.nuget/NuGet"
NUGET_CONFIG_FILE="$NUGET_CONFIG_DIR/NuGet.Config"

mkdir -p "$NUGET_CONFIG_DIR"

cat > "$NUGET_CONFIG_FILE" << 'EOF'
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" protocolVersion="3" />
  </packageSources>
  <config>
    <add key="http_proxy" value="http://127.0.0.1:8888" />
    <add key="https_proxy" value="http://127.0.0.1:8888" />
  </config>
</configuration>
EOF

echo "NuGet configuration updated at $NUGET_CONFIG_FILE"

# Start NuGet proxy forwarder in background
# This handles the proxy authentication that .NET's HttpClient doesn't support
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROXY_FORWARDER="$SCRIPT_DIR/nuget-proxy-forwarder.py"

if [ -f "$PROXY_FORWARDER" ]; then
    # Check if forwarder is already running
    if ! pgrep -f "nuget-proxy-forwarder.py" > /dev/null; then
        echo "Starting NuGet proxy forwarder on localhost:8888..."
        nohup python3 "$PROXY_FORWARDER" > /dev/null 2>&1 &
        sleep 1
        if pgrep -f "nuget-proxy-forwarder.py" > /dev/null; then
            echo "Proxy forwarder started successfully"
        else
            echo "Warning: Proxy forwarder may not have started correctly"
        fi
    else
        echo "NuGet proxy forwarder is already running"
    fi
else
    echo "Warning: Proxy forwarder script not found at $PROXY_FORWARDER"
fi
