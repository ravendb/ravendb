#!/bin/bash

function enable_cross_builds {
    echo "`date`" > $LOG
    echo "`date`: Installing packages..."
    sudo apt-get update >> ${LOG} 2>&1
    sudo apt-get install -y crossbuild-essential-armhf crossbuild-essential-arm64 cmake clang libxml2-dev fuse libbz2-dev libfuse-dev fuse >> ${LOG} 2>&1
    if [ $? -ne 0 ]; then echo "Failed. See ${LOG}";  exit 1; fi

    echo "`date`: Cloning osxcross..."
    rm -rf osxcross > /dev/null 2>&1
    git clone --single-branch --branch pointintime https://github.com/gregolsky/osxcross >> ${LOG} 2>&1
    if [ $? -ne 0 ]; then echo "Failed. See ${LOG}";  exit 1; fi

    echo "`date`: Copying MacOSX 10.11 SDK..."
    local MACOSX_SDK_TAR_PATH="${MACOSX_SDK_TAR_PATH:-build-libs/MacOSX10.11.sdk.tar.xz}"
    if [ ! -f "$MACOSX_SDK_TAR_PATH" ]; then 
        echo "Failed - MacOSX SDK tarball not found at $MACOSX_SDK_TAR_PATH. See ${LOG}" 
    fi

    cp "$MACOSX_SDK_TAR_PATH" osxcross/tarballs/ >> ${LOG} 2>&1
    pushd osxcross >> ${LOG} 2>&1
    if [ $? -ne 0 ]; then echo "Failed. See ${LOG}"; popd; exit 1; fi

    echo "`date`: Building mac compiler..."
    sed -i 's/read/echo/g' build.sh
    ./build.sh
    if [ $? -ne 0 ]; then echo "Failed."; popd; exit 1; fi
    popd >> ${LOG} 2&>1
    echo "Done."
    exit 0
}
