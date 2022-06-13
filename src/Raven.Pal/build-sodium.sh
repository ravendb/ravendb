#!/bin/bash

source ./build-libs/crossbuild.sh
source ./build-libs/colors.sh
source ./build-libs/libsodium.sh

OSXCROSS_SDKROOT="$(realpath "osxcross/target/SDK/${MACOS_SDK_DIRNAME}")"
export OSXCROSS_SDKROOT

libsodium_cross_build
