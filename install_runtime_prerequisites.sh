#!/bin/bash

UBUNTU_CODENAME=$(lsb_release -c | cut -d ":" -f2 | sed 's/\t//g')
UBUNTU_VERSION=$(lsb_release -r | cut -d ":" -f2 | sed 's/\t//g')

if [[ $UID != 0 ]]; then
    echo "Please run this script with sudo:"
    echo "sudo $0 $*"
    exit 1
fi

# install .netcore runtime dependencies
sudo apt-get install -y --no-install-recommends libunwind8 ca-certificates libcurl3

if [ "$UBUNTU_VERSION" = "14.04" ] ; then
    sudo apt-get install -y libicu52 
elif [ "$UBUNTU_VERSION" = "16.04" ] ; then
    sudo apt-get install -y libicu55
elif [ "$UBUNTU_VERSION" = "18.04" ] ; then
    sudo apt-get install -y libicu60
fi
