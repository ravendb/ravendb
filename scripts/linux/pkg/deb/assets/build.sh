#!/bin/bash -x

ASSETS_DIR=/assets
DEST_DIR=/build
release=$(lsb_release -sr | cut -d. -f1)

if [[ $release -ge 22 ]]; then
    mv -v $ASSETS_DIR/ravendb/debian/control_22 $ASSETS_DIR/ravendb/debian/control
else
    apt install dh-systemd
    mv -v $ASSETS_DIR/ravendb/debian/control_legacy $ASSETS_DIR/ravendb/debian/control
fi

rm -v $ASSETS_DIR/ravendb/debian/control_*
export RAVENDB_VERSION_MINOR=$( egrep -o -e '^[0-9]+.[0-9]+' <<< "$RAVENDB_VERSION" )

set -e

MS_DEB_NAME="/cache/packages-microsoft-prod_${DISTRO_VERSION}.deb"
if ! (test -f "${MS_DEB_NAME}" \
    || wget -O "${MS_DEB_NAME}" "https://packages.microsoft.com/config/ubuntu/${DISTRO_VERSION}/packages-microsoft-prod.deb"); then
     echo "Could not obtain packages-microsoft-prod.deb"
     exit 1
fi

dpkg -i $MS_DEB_NAME
apt update

DOWNLOAD_URL=${DOWNLOAD_URL:-"https://daily-builds.s3.amazonaws.com/RavenDB-${RAVENDB_VERSION}-${RAVEN_PLATFORM}.tar.bz2"}

export TARBALL="ravendb-${RAVENDB_VERSION}-${RAVEN_PLATFORM}.tar.bz2"
export CACHED_TARBALL="${TARBALL_CACHE_DIR}/${TARBALL}"

if ! (test -f "${CACHED_TARBALL}" || wget -O ${CACHED_TARBALL} -N --progress=dot:mega ${DOWNLOAD_URL}); then
    echo "Failed to download $DOWNLOAD_URL"
    exit 1
fi

DOTNET_FULL_VERSION=$(tar xf ${CACHED_TARBALL} RavenDB/runtime.txt -O | sed 's/\r$//' | sed -n "s/.NET Core Runtime: \([0-9.]\)/\1/p" )
DOTNET_VERSION_MINOR=$(egrep -o -e '^[0-9]+.[0-9]+' <<< $DOTNET_FULL_VERSION)
export DOTNET_DEPS_VERSION="$DOTNET_FULL_VERSION"
export DOTNET_RUNTIME_VERSION="$DOTNET_VERSION_MINOR"

if [[ $release -eq 24 && $RAVEN_PLATFORM == "raspberry-pi" ]]; then
    DOTNET_RUNTIME_DEPS="libicu74, libc6 (>= 2.38), libgcc-s1 (>= 3.0), liblttng-ust1t64 (>= 2.13.0), libssl3t64 (>= 3.0.0), libstdc++6 (>= 13.1), zlib1g (>= 1:1.1.4)"
else
    if [[ $release -ge 24 ]]; then
        DOTNET_RUNTIME_DEPS_PKG="dotnet-runtime-$DOTNET_RUNTIME_VERSION"
    else
        # Show dependencies for amd64 since that's the only platform Microsoft ships package for,
        # however the dependencies are the same at the moment.
        DOTNET_RUNTIME_DEPS_PKG="dotnet-runtime-deps-$DOTNET_RUNTIME_VERSION:amd64"
    fi
    
    # get depenencies and remove dotnet-host* dependencies
    DOTNET_RUNTIME_DEPS=$(apt show $DOTNET_RUNTIME_DEPS_PKG 2>/dev/null | sed -n -e 's/Depends: //p' | sed -E 's/(^|, )dotnet-host[^,]*(, |$)/\1/; s/, $//')
    if [ -z "$DOTNET_RUNTIME_DEPS" ]; then
        echo "Could not extract dependencies from $DOTNET_RUNTIME_DEPS_PKG package."
        exit 1
    fi
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
export GENERATEDFILENAME="ravendb_${PACKAGEVERSION}_${DEB_ARCHITECTURE}.deb"
export PACKAGEFILENAME="ravendb_${PACKAGEVERSION}_ubuntu.${DISTRO_VERSION}_${DEB_ARCHITECTURE}.deb"

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

export DEB_BUILD_OPTIONS="nostrip"
dpkg-buildpackage -us -uc -b 

cp -v "/build/$GENERATEDFILENAME" "$OUTPUT_DIR/$PACKAGEFILENAME"
chmod a+rw "$OUTPUT_DIR/$PACKAGEFILENAME"

echo "Package contents:"
dpkg -c "${OUTPUT_DIR}/${PACKAGEFILENAME}" | tee $OUTPUT_DIR/deb_contents.txt

echo "Package info:"
dpkg -I "${OUTPUT_DIR}/${PACKAGEFILENAME}" | tee $OUTPUT_DIR/deb_info.txt
