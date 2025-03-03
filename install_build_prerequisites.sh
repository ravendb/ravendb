#!/bin/bash

CURL_CMD=$(command -v curl)
GIT_CMD=$(command -v git)
NODE_CMD=$(command -v node)
POWERSHELL_CMD=$(command -v pwsh)
MONO_CMD=$(command -v mono)
DOTNET_VERSION_CMD=`dotnet --version 2> /dev/null`
UBUNTU_CODENAME=$(lsb_release -c | cut -d ":" -f2 | sed 's/\t//g')
UBUNTU_VERSION=$(lsb_release -r | cut -d ":" -f2 | sed 's/\t//g')

# Introductory message
echo "Starting environment setup for RavenDB build on Ubuntu $UBUNTU_VERSION ($UBUNTU_CODENAME)"

# Get the full path of the script
SCRIPT_PATH=$(realpath "$0")

# Check if the script is running with root privileges
if [ "$EUID" -ne 0 ]; then
    echo "This script must be run as root. Please use sudo."
    echo "sudo $SCRIPT_PATH $*"
    exit 1
fi

# Check Ubuntu version
if ! echo "16.04 18.04 20.04 22.04 24.04" | grep -q "$UBUNTU_VERSION"; then
    echo "Unsupported Ubuntu version: $UBUNTU_VERSION $UBUNTU_CODENAME. Must be 16.04, 18.04, 20.04, 22.04, or 24.04."
    exit 1
fi

# Check and install curl
echo "Checking for curl..."
CURL_CMD=$(command -v curl)
if [ -z "$CURL_CMD" ]; then
    echo "curl not found. Installing curl..."
    apt-get install -y curl
else
    echo "curl is already installed."
fi

# Set up Microsoft repository
echo "Setting up Microsoft repository for .NET and PowerShell..."
curl -sSL https://packages.microsoft.com/config/ubuntu/$UBUNTU_VERSION/packages-microsoft-prod.deb -o packages-microsoft-prod.deb
dpkg -i packages-microsoft-prod.deb
rm packages-microsoft-prod.deb
apt-get update
sudo apt-get install -y dotnet-sdk-5.0

mkdir ./dotnet_tmp
cd ./dotnet_tmp
sudo dotnet new console
sudo dotnet build #dotnet telemetry
cd ..
sudo rm -rf ./dotnet_tmp

if [ -z "$POWERSHELL_CMD" ] ; then
    echo "Powershell not found. Installing.."

    if [ -z "$CURL_CMD" ]; then
        sudo apt-get install -y curl 
    fi

    if [ "$UBUNTU_VERSION" = "16.04" ] ; then
        curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list | sudo tee /etc/apt/sources.list.d/microsoft.list
    elif [ "$UBUNTU_VERSION" = "14.04" ] ; then
        curl https://packages.microsoft.com/config/ubuntu/14.04/prod.list | sudo tee /etc/apt/sources.list.d/microsoft.list
    elif [ "$UBUNTU_VERSION" = "18.04" ] ; then
        curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list | sudo tee /etc/apt/sources.list.d/microsoft.list
    elif [ "$UBUNTU_VERSION" = "20.04" ] ; then
        curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | sudo tee /etc/apt/sources.list.d/microsoft.list                    
    fi
    
    sudo apt-get update
    sudo apt-get install -y powershell
else
    echo "Powershell is installed."
fi

if [ -z "$NODE_CMD" ] ; then
    echo "Node not found. Installing.."

    if [ -z "$CURL_CMD" ]; then
        sudo apt-get install -y curl 
    fi

    curl -sL https://deb.nodesource.com/setup_8.x | sudo -E bash -
    sudo apt-get install -y nodejs build-essential
else
    NODE_VERSION="$($NODE_CMD --version)"

    if [[ ! "$NODE_VERSION" =~ ^v?(8|9|10|11) ]] ; then
        echo "Incompatible version of NodeJS found: $NODE_VERSION. NodeJS 8.x or later is required."
        exit 1
    else
        echo "Node $NODE_VERSION is installed."
    fi
fi

if [ -z "$GIT_CMD" ]; then
    sudo apt-get install -y git
fi

echo "To build RavenDB run: ./build.sh"
