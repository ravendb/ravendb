#!/bin/bash

# Variable definitions
UBUNTU_VERSION=$(lsb_release -rs 2>/dev/null)
UBUNTU_CODENAME=$(lsb_release -cs 2>/dev/null)

# Introductory message
if [ -n "$UBUNTU_VERSION" ]; then
    echo "Starting environment setup for RavenDB build on Ubuntu $UBUNTU_VERSION ($UBUNTU_CODENAME)"
else
    echo "Starting environment setup for RavenDB build (non-Ubuntu or lsb_release not available)"
fi

# Determine how to run privileged commands
APT_PREFIX=""
if [ "$EUID" -eq 0 ]; then
    APT_PREFIX=""
elif command -v sudo &> /dev/null; then
    APT_PREFIX="sudo"
else
    echo "Note: Not running as root and sudo is not available."
    echo "Will install tools using non-root methods where possible."
    echo "Some packages (curl, jq, git) may need to be installed manually if not already present."
    echo ""
fi

apt_install() {
    if [ -n "$APT_PREFIX" ] || [ "$EUID" -eq 0 ]; then
        $APT_PREFIX apt-get install -y "$@"
    else
        echo "WARNING: Cannot install $* via apt-get (no root/sudo). Please install manually."
        return 1
    fi
}

# Update package lists if we have apt access
if [ -n "$APT_PREFIX" ] || [ "$EUID" -eq 0 ]; then
    echo "Updating package lists..."
    $APT_PREFIX apt-get update
fi

# Check and install curl
echo "Checking for curl..."
if ! command -v curl &> /dev/null; then
    echo "curl not found. Installing curl..."
    apt_install curl || { echo "Error: curl is required and could not be installed."; exit 1; }
else
    echo "curl is already installed."
fi

# Check and install jq for parsing JSON
echo "Checking for jq..."
if ! command -v jq &> /dev/null; then
    echo "jq not found. Installing jq..."
    apt_install jq || { echo "Error: jq is required and could not be installed."; exit 1; }
else
    echo "jq is already installed."
fi

# Extract .NET SDK version from global.json
echo "Extracting .NET SDK version from global.json..."
if [ ! -f "global.json" ]; then
    echo "Error: global.json not found. Please run this script from the RavenDB repository root directory."
    exit 1
fi

SDK_VERSION=$(jq -r '.sdk.version' global.json)
if [ -z "$SDK_VERSION" ] || [ "$SDK_VERSION" == "null" ]; then
    echo "Error: Failed to parse .NET SDK version from global.json."
    exit 1
fi

# Derive major.minor version (e.g., 8.0 from 8.0.100)
MAJOR_MINOR_VERSION=$(echo "$SDK_VERSION" | awk -F. '{print $1"."$2}')
if [ -z "$MAJOR_MINOR_VERSION" ]; then
    echo "Error: Unable to determine .NET SDK major.minor version from '$SDK_VERSION'."
    exit 1
fi

# Check and install .NET SDK
echo "Checking for .NET SDK..."
CURRENT_DOTNET_VERSION=$(dotnet --version 2>/dev/null)
if [ -n "$CURRENT_DOTNET_VERSION" ]; then
    CURRENT_MAJOR_MINOR=$(echo "$CURRENT_DOTNET_VERSION" | awk -F. '{print $1"."$2}')
    if [ "$CURRENT_MAJOR_MINOR" == "$MAJOR_MINOR_VERSION" ]; then
        echo ".NET SDK $CURRENT_DOTNET_VERSION is already installed and compatible."
    else
        echo ".NET SDK $CURRENT_DOTNET_VERSION found but need $MAJOR_MINOR_VERSION."
        echo "Installing .NET SDK $MAJOR_MINOR_VERSION..."
        if ! apt_install dotnet-sdk-$MAJOR_MINOR_VERSION; then
            echo "Trying dotnet-install.sh (non-root method)..."
            curl -fsSL https://dot.net/v1/dotnet-install.sh | bash -s -- --channel "$MAJOR_MINOR_VERSION"
            export PATH="$HOME/.dotnet:$PATH"
        fi
    fi
else
    echo ".NET SDK not found. Installing .NET SDK $MAJOR_MINOR_VERSION..."
    if ! apt_install dotnet-sdk-$MAJOR_MINOR_VERSION; then
        echo "Trying dotnet-install.sh (non-root method)..."
        curl -sSL https://dot.net/v1/dotnet-install.sh | bash -s -- --channel "$MAJOR_MINOR_VERSION"
        export PATH="$HOME/.dotnet:$PATH"
    fi
fi

# Check and install PowerShell
echo "Checking for PowerShell..."
if ! command -v pwsh &> /dev/null; then
    echo "PowerShell not found. Installing PowerShell..."
    if ! apt_install powershell; then
        echo "Installing PowerShell via dotnet tool..."
        dotnet tool install --global PowerShell
        export PATH="$PATH:$HOME/.dotnet/tools"
    fi
else
    echo "PowerShell is already installed."
fi

# Check and install Node.js
echo "Checking for Node.js..."
NODE_CMD=$(command -v node)
if [ -z "$NODE_CMD" ]; then
    echo "Node.js not found. Installing Node.js..."
    if [ -n "$APT_PREFIX" ] || [ "$EUID" -eq 0 ]; then
        curl -fsSL https://deb.nodesource.com/setup_lts.x | $APT_PREFIX bash -
        apt_install nodejs build-essential
    else
        echo "Installing Node.js via fnm (non-root method)..."
        curl -fsSL https://fnm.vercel.app/install | bash
        export PATH="$HOME/.local/share/fnm:$PATH"
        eval "$(fnm env)"
        fnm install --lts
    fi
else
    NODE_VERSION="$($NODE_CMD --version)"
    MAJOR_VERSION=$(echo "$NODE_VERSION" | sed 's/^v\?\([0-9]*\)\..*/\1/')
    if [ "$MAJOR_VERSION" -lt 20 ]; then
        echo "Incompatible Node.js version found: $NODE_VERSION. Node.js 20.x or later is required."
        exit 1
    else
        echo "Node.js $NODE_VERSION is installed and compatible."
    fi
fi

# Check and install git
echo "Checking for git..."
if ! command -v git &> /dev/null; then
    echo "git not found. Installing git..."
    apt_install git || { echo "Error: git is required and could not be installed."; exit 1; }
else
    echo "git is already installed."
fi

# Completion message
echo ""
echo "Environment setup complete. To build RavenDB run: ./build.sh"
