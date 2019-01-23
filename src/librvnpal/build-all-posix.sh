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
	echo "`date`" > $LOG
	echo "`date`: Installing packages..."
	sudo apt-get install -y crossbuild-essential-armhf cmake clang libxml2-dev fuse libbz2-dev libfuse-dev fuse >> ${LOG} 2>&1
	if [ $? -ne 0 ]; then echo "Failed. See ${LOG}";  exit 1; fi
	echo "`date`: Cloning osxcross..."
	rm -rf osxcross > /dev/null 2>&1
	git clone --single-branch --branch pointintime https://github.com/aviviadi/osxcross >> ${LOG} 2>&1
	if [ $? -ne 0 ]; then echo "Failed. See ${LOG}";  exit 1; fi
	echo "`date`: Copying MacOSX 10.11 SDK..."
	cp MacOSX10.11.sdk.tar.xz osxcross/tarballs/ >> ${LOG} 2>&1
	pushd osxcross >> ${LOG} 2>&1
	if [ $? -ne 0 ]; then echo "Failed. See ${LOG}"; popd; exit 1; fi
	echo "`date`: Building mac compiler..."
	sed -i 's/read/echo/g' build.sh
	./build.sh
	if [ $? -ne 0 ]; then echo "Failed."; popd; exit 1; fi
	popd >> ${LOG} 2&>1
	echo "Done."
	exit 0
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
	./make.sh cross linux-x64 clean
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
