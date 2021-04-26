#!/bin/bash -x

ASSETS_DIR=/assets
DEST_DIR=/build

export RAVENDB_VERSION_MINOR=$( egrep -o -e '^[0-9]+.[0-9]+' <<< "$RAVENDB_VERSION" )


set -e

DOWNLOAD_URL=https://daily-builds.s3.amazonaws.com/RavenDB-${RAVENDB_VERSION}-${RAVEN_PLATFORM}.tar.bz2 

export TARBALL="ravendb-${RAVENDB_VERSION}-${RAVEN_PLATFORM}.tar.bz2"
export CACHED_TARBALL="${TARBALL_CACHE_DIR}/${TARBALL}"

test -f "${CACHED_TARBALL}" || wget -O ${CACHED_TARBALL} -N --progress=dot:mega ${DOWNLOAD_URL}

DOTNET_FULL_VERSION=`tar xf ${CACHED_TARBALL} RavenDB/runtime.txt -O | sed 's/\r$//' | sed -n "s/.NET Core Runtime: \([0-9.]\)/\1/p" `
DOTNET_VERSION_MINOR=$(egrep -o -e '^[0-9]+.[0-9]+' <<< $DOTNET_FULL_VERSION)
export DOTNET_DEPS_VERSION="$DOTNET_FULL_VERSION"
export DOTNET_RUNTIME_VERSION="$DOTNET_VERSION_MINOR"

# Show dependencies for amd64 since that's the only platform Microsoft ships package for,
# however the dependencies are the same at the moment.
DOTNET_RUNTIME_DEPS_PKG="dotnet-runtime-deps-$DOTNET_RUNTIME_VERSION:amd64"
DOTNET_RUNTIME_DEPS=`apt show $DOTNET_RUNTIME_DEPS_PKG 2>/dev/null | sed -n -e 's/Depends: //p'`
if [ -z "$DOTNET_RUNTIME_DEPS" ]; then
    echo "Could not extract dependencies from $DOTNET_RUNTIME_DEPS_PKG package."
    exit 1
fi

export DEB_DEPS="${DOTNET_RUNTIME_DEPS}, libc6-dev (>= 2.27)"

echo ".NET Runtime: $DOTNET_FULL_VERSION"
echo "Package dependencies: $DEB_DEPS"

case $RAVEN_PLATFORM in

    linux-x64)
        export DEB_ARCHITECTURE="amd64"
        export RAVEN_SO_ARCH_SUFFIX="linux.x64"
        ;;

    linux-arm64)
        export DEB_ARCHITECTURE="arm64"
        export RAVEN_SO_ARCH_SUFFIX="arm.64"
        ;;

    raspberry-pi)
        export DEB_ARCHITECTURE="armhf"
        export RAVEN_SO_ARCH_SUFFIX="arm.32"
        ;;
    
    *)
        echo "Unsupported platform $RAVEN_PLATFORM for building a DEB package for."
        exit 1

esac

export VERSION=$RAVENDB_VERSION
export PACKAGE_REVISION="${PACKAGE_REVISION:-0}"
export PACKAGEVERSION="${VERSION}-${PACKAGE_REVISION}"
export PACKAGEFILENAME="ravendb_${PACKAGEVERSION}_${DEB_ARCHITECTURE}.deb"

export DEBCHANGELOGDATE=$(date +"%a, %d %b %Y %H:%M:%S %z")
export COPYRIGHT_YEAR=$(date +"%Y")

for asset in /assets/ravendb/debian/{control,copyright,changelog}
do
    dest="$DEST_DIR/ravendb/debian/$(basename $asset)"
    mkdir -p $(dirname $dest)
    envsubst < $asset > $dest
done

find /build -type d -exec chmod 755 {} +
find /build -type f -exec chmod 644 {} +

dpkg-buildpackage -us -uc -b 

cp -v /build/*.deb $OUTPUT_DIR 

echo "Package contents:"
dpkg -c "${OUTPUT_DIR}/${PACKAGEFILENAME}" | tee $OUTPUT_DIR/deb_contents.txt

echo "Package info:"
dpkg -I "${OUTPUT_DIR}/${PACKAGEFILENAME}" | tee $OUTPUT_DIR/deb_info.txt
