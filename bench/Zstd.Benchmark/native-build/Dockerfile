# Same base image and cross toolchains as src/Raven.Pal/build-libs/zstd-build.Dockerfile (minus the osxcross stage,
# which needs the private macOS SDK tarball), so locally built binaries match the shipped ones.
FROM ubuntu:18.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates git make build-essential \
    gcc-mingw-w64 binutils-mingw-w64 \
    crossbuild-essential-armhf crossbuild-essential-arm64 \
    && rm -rf /var/lib/apt/lists/*

ENV ARTIFACTS_DIR="/out"
WORKDIR /build
