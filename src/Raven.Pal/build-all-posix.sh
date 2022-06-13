#!/bin/bash

if [ $# -ne 1 ] || [ "$1" == "-h" ]; then
	echo "usage: build-all-posix.sh <setup | build>"
	echo "		setup - first time use, install packages for arm compiling and build osx compiler"
	echo "		build - try to build for all supported posix platforms"
	echo " "
	exit 1
fi

function log {
   echo "= [$(date --iso-8601=seconds)]: $1"
}

export -f log

LOG="$(realpath .)/build.log"
export LOG

# shellcheck source="./build-libs/crossbuild.sh"
source "./build-libs/crossbuild.sh"

if [ "$1" == "setup" ]; then
    enable_cross_builds
elif [ "$1" != "build" ]; then
	echo "Invalid usage"
	exit 1
fi

STAT=0

OSXCROSS_SDKROOT="$(realpath "osxcross/target/SDK/${MACOS_SDK_DIRNAME}")"
if [ ! -d "$OSXCROSS_SDKROOT" ]; then
  log "SDK does not exist under path $OSXCROSS_SDKROOT"
fi
export OSXCROSS_SDKROOT

[ -d artifacts ] || mkdir artifacts
archs=( linux-x64 linux-arm linux-arm64 osx-x64 osx-arm64 )
for arch in "${archs[@]}"; do
	
	log "Build for $arch"
	
	./make.sh cross "$arch" skip-copy
	
	STATUS_C=$?
	STAT=$(expr ${STAT} + ${STATUS_C} )
	
	mv -v ./*.so artifacts/ 
	mv -v ./*.dylib artifacts/ 

	./make.sh cross "$arch" clean
done

log ""
log ""

if [ "${STAT}" -ne 0 ]; then
	log "Build FAILED!"
	exit 1
fi

echo "Build Success!"
rm -v "${LOG}"

exit 0
