#!/bin/bash -ex

scriptRoot=$(dirname $0)

if [ -z "$OUTPUT_DIR" ]; then
  export OUTPUT_DIR="$(pwd)/dist"
fi

if [ -z "$TEMP_DIR" ]; then
  export TEMP_DIR="$(pwd)/temp"
fi


required=( DISTRO_NAME DISTRO_VERSION DISTRO_VERSION_NAME RAVENDB_VERSION RAVEN_PLATFORM DOCKER_BUILDPLATFORM )

for required_var in ${required[@]}; do
    if [ -z "${!required_var}" ]; then
        echo "Required parameter $required_var is not set."
        exit 1
    fi
done


ravenVersion="$RAVENDB_VERSION"
distName="$DISTRO_NAME"
distVer="$DISTRO_VERSION"
distVerName="$DISTRO_VERSION_NAME"
outputDir="$OUTPUT_DIR" 

echo "Build DEB of RavenDB $ravenVersion for distro $distName $distVer $distVerName $DEB_ARCHITECTURE"

if [ "$DEB_ARCHITECTURE" == "amd64" ]; then
    DOCKER_FILE="./ubuntu_amd64.Dockerfile"
else
    DOCKER_FILE="./ubuntu_multiarch.Dockerfile"
fi

DEB_BUILD_ENV_IMAGE="ravendb-deb_ubuntu_$DEB_ARCHITECTURE"

docker build \
    --platform $DOCKER_BUILDPLATFORM \
    --build-arg "DISTRO_VERSION_NAME=$DISTRO_VERSION_NAME" \
    --build-arg "DISTRO_VERSION=$DISTRO_VERSION" \
    --build-arg "QEMU_ARCH=$QEMU_ARCH" \
    -t $DEB_BUILD_ENV_IMAGE \
    -f $DOCKER_FILE .

if [ $? -ne 0 ]; then
    echo "Failed to build the DEB build environment image."
    exit 1
fi


mkdir -p -v "$TEMP_DIR" "$OUTPUT_DIR/$DISTRO_VERSION"

docker run --rm \
    --platform $DOCKER_BUILDPLATFORM \
    -v "$OUTPUT_DIR:/dist" \
    -v "$TEMP_DIR:/cache" \
    -e RAVENDB_VERSION=$ravenVersion  \
    -e "DOTNET_RUNTIME_VERSION=$DOTNET_RUNTIME_VERSION" \
    -e "DOTNET_DEPS_VERSION=$DOTNET_DEPS_VERSION" \
    -e "DISTRO_VERSION_NAME=$DISTRO_VERSION_NAME" \
    -e "DISTRO_VERSION=$DISTRO_VERSION" \
    -e "RAVEN_PLATFORM=$RAVEN_PLATFORM" \
    -e "QEMU_ARCH=$QEMU_ARCH" \
    $DEB_BUILD_ENV_IMAGE 

