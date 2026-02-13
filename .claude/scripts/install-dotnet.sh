#!/bin/bash
# Install .NET 10 SDK and essential build tools for development
# This script is intended to be run as a Claude Code startup hook

set -e

DOTNET_VERSION="10.0"
INSTALL_DIR="$HOME/.dotnet"

# Install essential build tools if missing
if ! command -v tail &> /dev/null || ! command -v head &> /dev/null; then
    echo "Installing essential build tools (coreutils)..."
    apt-get update -qq && apt-get install -y -qq coreutils > /dev/null 2>&1 || true
fi

# Check if .NET 10 is already installed
if command -v dotnet &> /dev/null; then
    INSTALLED_VERSION=$(dotnet --version 2>/dev/null || echo "")
    if [[ "$INSTALLED_VERSION" == 10.* ]]; then
        echo ".NET 10 SDK is already installed (version: $INSTALLED_VERSION)"
        exit 0
    fi
fi

echo "Installing .NET $DOTNET_VERSION SDK..."

# Download and run the official Microsoft install script
curl -sSL https://dot.net/v1/dotnet-install.sh -o /tmp/dotnet-install.sh
chmod +x /tmp/dotnet-install.sh

# Install .NET 10 SDK
/tmp/dotnet-install.sh --channel $DOTNET_VERSION --install-dir "$INSTALL_DIR"

# Clean up
rm -f /tmp/dotnet-install.sh

# Add to PATH for current session
export DOTNET_ROOT="$INSTALL_DIR"
export PATH="$INSTALL_DIR:$PATH"

# Verify installation
echo ".NET SDK installed successfully:"
"$INSTALL_DIR/dotnet" --version

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
