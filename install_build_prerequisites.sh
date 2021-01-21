#!/bin/bash

CURL_CMD=$(which curl)
GIT_CMD=$(which git)
NODE_CMD=$(which node)
POWERSHELL_CMD=$(which pwsh)
MONO_CMD=$(which mono)
DOTNET_VERSION_CMD=`dotnet --version 2> /dev/null`
UBUNTU_CODENAME=$(lsb_release -c | cut -d ":" -f2 | sed 's/\t//g')
UBUNTU_VERSION=$(lsb_release -r | cut -d ":" -f2 | sed 's/\t//g')

if [[ ! "$UBUNTU_VERSION" =~ ^1[468]\.04$ ]] ; then
    echo "Unsupported Ubuntu version: $UBUNTU_VERSION $UBUNTU_CODENAME. Must be 20.04, 18.04, 16.04 or 14.04."
    exit -1
fi


if [[ $UID != 0 ]]; then
    echo "Please run this script with sudo:"
    echo "sudo $0 $*"
    exit 1
fi

eval 

echo "Installing .NET Core SDK 5.0"

if [ -z "$CURL_CMD" ]; then
    sudo apt-get install -y curl 
fi

curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -

if [ "$UBUNTU_VERSION" = "16.04" ] ; then
    sudo sh -c 'echo "deb [arch=amd64] https://packages.microsoft.com/repos/microsoft-ubuntu-xenial-prod xenial main" > /etc/apt/sources.list.d/dotnetdev.list'
elif [ "$UBUNTU_VERSION" = "14.04" ] ; then
    sudo sh -c 'echo "deb [arch=amd64] https://packages.microsoft.com/repos/microsoft-ubuntu-trusty-prod trusty main" > /etc/apt/sources.list.d/dotnetdev.list'
elif [ "$UBUNTU_VERSION" = "18.04" ] ; then
    sudo sh -c 'echo "deb [arch=amd64] https://packages.microsoft.com/repos/microsoft-ubuntu-bionic-prod bionic main" > /etc/apt/sources.list.d/dotnetdev.list'
elif [ "$UBUNTU_VERSION" = "20.04" ] ; then
    sudo sh -c 'echo "deb [arch=amd64] https://packages.microsoft.com/repos/microsoft-ubuntu-focal-prod focal main" > /etc/apt/sources.list.d/dotnetdev.list'          
fi

sudo apt-get update
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
