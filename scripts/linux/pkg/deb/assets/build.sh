#!/bin/bash -x

ASSETS_DIR=/assets
DEST_DIR=/build

export RAVENDB_VERSION_MINOR=$( egrep -o -e '^[0-9]+.[0-9]+' <<< "$RAVENDB_VERSION" )

export VERSION=$RAVENDB_VERSION
export PACKAGE_REVISION="${PACKAGE_REVISION:-0}"
export PACKAGEVERSION="${VERSION}-${PACKAGE_REVISION}"
export PACKAGEFULLVERSION="${VERSION}-${PACKAGE_REVISION}~${DISTRIBUTION}0"
export DEBCHANGELOGDATE=$(date +"%a, %d %b %Y %H:%M:%S %z")
export PACKAGEFILENAME="ravendb_${PACKAGEVERSION}_${DEB_ARCHITECTURE}.deb"

set -e

for asset in /assets/ravendb/debian/{control,copyright,changelog}
do
    dest="$DEST_DIR/ravendb/debian/$(basename $asset)"
    mkdir -p $(dirname $dest)
    envsubst < $asset > $dest
done

find /build -type d -exec chmod 755 {} +
find /build -type f -exec chmod 644 {} +

ls -l

dpkg-buildpackage -us -uc -b 

cp -v /build/*.deb $OUTPUT_DIR 
dpkg -c "${OUTPUT_DIR}/${PACKAGEFILENAME}" > $OUTPUT_DIR/deb_pkg_files.txt
