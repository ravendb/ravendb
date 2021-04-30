#!/bin/bash -x

export DISTRO_NAME="ubuntu"
export DISTRO_VERSION="18.04"
export DISTRO_VERSION_NAME="bionic"

source "$(dirname $0)/set-raven-platform-armhf.sh"
source "$(dirname $0)/set-raven-version-env.sh"

./build-deb.sh
