#!/bin/bash

if [[ $UID != 0 ]]; then
    echo "Please run this script with sudo:"
    echo "sudo $0 $*"
    exit 1
fi

# install .netcore runtime dependencies
sudo apt-get install -y --no-install-recommends libunwind8 libicu55 libcurl3 ca-certificates
