#!/bin/bash -x

source "$(dirname $0)/set-ubuntu-bionic.sh"
source "$(dirname $0)/set-raven-platform-armhf.sh"
source "$(dirname $0)/set-raven-version-env.sh"

./build-deb.sh
