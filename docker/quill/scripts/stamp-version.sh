#!/usr/bin/env sh
set -e

VERSION="$1"
[ -z "$VERSION" ] && exit 0

sed -i -E "s/AssemblyInformationalVersion\(\"[^\"]*\"\)/AssemblyInformationalVersion(\"$VERSION\")/" \
    src/CommonAssemblyInfo.cs

sed -i -E "s/(FullVersion = )\"[^\"]*\"/\1\"$VERSION\"/" \
    src/Raven.Client/Properties/VersionInfo.cs

echo "stamp-version: set version to $VERSION"
