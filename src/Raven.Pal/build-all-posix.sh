#!/bin/bash
LOG=build-all-posix.log
if [ $# -ne 1 ] || [ "$1" == "-h" ]; then
	echo "usage: build-all-posix.sh <setup | build>"
	echo "		setup - first time use, install packages for arm compiling and build osx compiler"
	echo "		build - try to build for all supported posix platforms"
	echo " "
	exit 1
fi

if [ "$1" == "setup" ]; then
    source ./build-libs/crossbuild.sh
    enable_cross_builds
elif [ "$1" != "build" ]; then
	echo "Invalid usage"
	exit 1
fi

STAT=0
[ -d artifacts ] || mkdir artifacts
for arch in linux-x64 linux-arm linux-arm64 osx-x64 ; do
	./make.sh cross $arch skip-copy
	STATUS_C=$?
	STAT=$(expr ${STAT} + ${STATUS_C})
	mv *.so artifacts/ > /dev/null 2>&1
	mv *.dylib artifacts/ > /dev/null 2>&1
	./make.sh cross $arch clean
done
echo ""
echo ""
if [ ${STAT} -ne 0 ]; then
	echo "Build FAILED!"
	exit 1
fi
echo "Build Success!"
rm ${LOG} > /dev/null 2>&1
exit 0
