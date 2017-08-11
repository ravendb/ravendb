#!/bin/bash

CURL_CMD=$(which curl)
GIT_CMD=$(which git)
NODE_CMD=$(which node)
POWERSHELL_CMD=$(which powershell)
MONO_CMD=$(which mono)
DOTNET_VERSION_CMD=`dotnet --version 2> /dev/null`
UBUNTU_CODENAME=$(lsb_release -c | cut -d ":" -f2 | sed 's/\t//g')
UBUNTU_VERSION=$(lsb_release -r | cut -d ":" -f2 | sed 's/\t//g')

if [[ ! "$UBUNTU_VERSION" =~ ^1[46]\.04$ ]] ; then
    echo "Unsupported Ubuntu version: $UBUNTU_VERSION $UBUNTU_CODENAME. Must be 16.04 or 14.04."
    exit -1
fi


if [[ $UID != 0 ]]; then
    echo "Please run this script with sudo:"
    echo "sudo $0 $*"
    exit 1
fi

eval 

if [[ ! "$DOTNET_VERSION_CMD" =~ ^1\.0\.[4-9]$ ]] ; then
    echo ".NETcore 1.0.4 not found. Installing.."
    sudo echo "deb [arch=amd64] https://apt-mo.trafficmanager.net/repos/dotnet-release/ $UBUNTU_CODENAME main" > /etc/apt/sources.list.d/dotnetdev.list
    sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 417A0893
    sudo apt-get update
    sudo apt-get install -y dotnet-dev-1.0.4
else
    echo ".NETcore 1.0.4 is installed."
fi

if [ -z "$POWERSHELL_CMD" ] ; then

    echo "Powershell not found. Installing.."

    if [ "$UBUNTU_VERSION" = "16.04" ] ; then
        POWERSHELL_URL="https://github.com/PowerShell/PowerShell/releases/download/v6.0.0-beta.1/powershell_6.0.0-beta.1-1ubuntu1.16.04.1_amd64.deb"
    elif [ "$UBUNTU_VERSION" = "14.04" ] ; then
        POWERSHELL_URL="https://github.com/PowerShell/PowerShell/releases/download/v6.0.0-beta.1/powershell_6.0.0-beta.1-1ubuntu1.14.04.1_amd64.deb"
    fi

    POWERSHELL_FILE="powershell.deb"

    wget "$POWERSHELL_URL" -O "$POWERSHELL_FILE"
    sudo dpkg -i "$POWERSHELL_FILE"
    sudo apt-get install -f -y
    rm "$POWERSHELL_FILE"
else
    echo "Powershell is installed."
fi

if [ -z "$NODE_CMD" ] ; then
    echo "Node not found. Installing.."

    if [ -z "$CURL_CMD" ]; then
        sudo apt-get install -y curl 
    fi

    curl -sL https://deb.nodesource.com/setup_6.x | sudo -E bash -
    sudo apt-get install -y nodejs build-essential
else
    NODE_VERSION="$($NODE_CMD --version)"

    if [[ ! "$NODE_VERSION" =~ ^v?[67] ]] ; then
        echo "Incompatible version of NodeJS found: $NODE_VERSION. NodeJS 6.x or later is required."
        exit 1
    else
    echo "Node $NODE_VERSION is installed."
    fi
fi

if [ -z "$MONO_CMD" ] ; then
    echo "Mono not found. Installing..."
    sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF
    echo "deb http://download.mono-project.com/repo/debian wheezy main" | sudo tee /etc/apt/sources.list.d/mono-xamarin.list
    sudo apt-get update
    sudo apt-get install -y mono-complete
else 
    echo "Mono is installed."
fi

if [ -z "$GIT_CMD" ]; then
    sudo apt-get install -y git
fi

echo "To build RavenDB run: powershell ./build.ps1"
