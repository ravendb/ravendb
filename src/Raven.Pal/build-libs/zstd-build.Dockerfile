FROM ubuntu:18.04 as build-osxcross
RUN apt update
RUN apt install -y sudo git
COPY crossbuild.sh MacOSX12.3.sdk.tar.xz ./
ENV MACOSX_SDK_TAR_PATH='MacOSX12.3.sdk.tar.xz'
RUN bash -c "export LOG=/dev/stdout && source ./crossbuild.sh && enable_cross_builds"

FROM ubuntu:18.04
RUN apt update
RUN apt install -y \
    git make libtool autoconf gcc-mingw-w64 sudo \
    crossbuild-essential-armhf crossbuild-essential-arm64 \
    cmake clang libxml2-dev fuse libbz2-dev libfuse-dev fuse

ENV ARTIFACTS_DIR="/build/artifacts"
WORKDIR /build

COPY --from=build-osxcross osxcross /osxcross
COPY zstd-build-deps.sh ./
RUN bash -c "source ./zstd-build-deps.sh && zstd_clone && zstd_install_build_deps"

COPY * ./

CMD ./build-zstd-posix.sh
