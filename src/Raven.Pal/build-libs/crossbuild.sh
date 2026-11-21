#!/bin/bash

export MACOS_SDK_VERSION=12.3
export MACOS_SDK_DIRNAME="MacOSX${MACOS_SDK_VERSION}.sdk"

function log {
   echo "$(date): $1" >> "$LOG"
}

function enable_cross_builds {
    log "Installing packages..."
    # noninteractive: a package like tzdata otherwise waits for an answer and the setup never ends
    sudo env DEBIAN_FRONTEND=noninteractive apt-get update -qq >> "${LOG}" 2>&1
    # build-essential: make and the c89 of the linux-x64 build; the rest of the second line builds osxcross
    if ! sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y crossbuild-essential-armhf crossbuild-essential-arm64 cmake clang libxml2-dev fuse libbz2-dev libfuse-dev libssl-dev \
        build-essential git wget ca-certificates xz-utils bzip2 cpio patch python3 liblzma-dev zlib1g-dev uuid-dev >> "${LOG}" 2>&1; then
        echo "Failed. See ${LOG}";  
        exit 1;
    fi

    log "Cloning osxcross..."
    rm -rf osxcross > /dev/null 2>&1
    if ! git clone --single-branch --branch ravendb https://github.com/gregolsky/osxcross >> ${LOG} 2>&1; then
        echo "Failed. See ${LOG}";  
        exit 1;
    fi
    
    log "Copying MacOSX SDK..."
    local MACOS_SDK_URL=https://ravendb-build-assets.s3.amazonaws.com/macos/MacOSX${MACOS_SDK_VERSION}.sdk.tar.xz
    local MACOS_SDK_TAR_PATH="${MACOSX_SDK_TAR_PATH:-build-libs/$MACOS_SDK_DIRNAME.tar.xz}"

    if [ ! -f "$MACOS_SDK_TAR_PATH" ]; then
        
        echo "SDK tarball not found. Downloading..."
        if ! wget -O "$MACOS_SDK_TAR_PATH" "$MACOS_SDK_URL"; then
            echo "Failed to download MacOSX SDK tarball. See ${LOG}" 
            exit 1
        fi
    fi

    cp -v "$MACOS_SDK_TAR_PATH" osxcross/tarballs/ >> "${LOG}" 2>&1
    pushd osxcross >> "${LOG}" 2>&1 || exit 1

    mkdir -p "target/SDK/${MACOS_SDK_DIRNAME}"

    OSXCROSS_SDKROOT="$(realpath "target/SDK/${MACOS_SDK_DIRNAME}")"
    export OSXCROSS_SDKROOT
    # build.sh extracts the SDK from tarballs/ into target/SDK

    log "Building mac compiler..."
    sed -i 's/read/echo/g' build.sh
    if ! ./build.sh; then
        echo "Failed."
        popd || exit 1
        exit 1
    fi
    
    popd >> "${LOG}" 2>&1 || exit 1
    echo "Done."
    exit 0
}
