#!/bin/bash
# Builds libzstd for macOS (x64 and arm64) natively on a Mac with the Xcode command line tools.
# Either CPU can build both architectures.
#
# Usage: build-zstd-macos.sh [output directory]     (default: libs/libzstd of this repository)
#        ZSTD_VERSION=v1.5.7 build-zstd-macos.sh

set -euo pipefail

ZSTD_VERSION="${ZSTD_VERSION:-v1.5.7}"
OUT_DIR="${1:-$(cd "$(dirname "$0")/../../../libs/libzstd" && pwd)}"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

git clone --quiet --depth 1 --branch "$ZSTD_VERSION" https://github.com/facebook/zstd.git "$WORK_DIR/zstd"

function build {
    local arch=$1
    local min_version=$2
    local name=$3
    local log="$WORK_DIR/build-$arch.log"

    make -C "$WORK_DIR/zstd/lib" clean > /dev/null
    # MOREFLAGS keeps the flags the zstd makefile uses for the shared library (-O3 -fPIC -fvisibility=hidden, multi-threaded)
    if ! make -C "$WORK_DIR/zstd/lib" -j"$(sysctl -n hw.ncpu)" lib-release \
        MOREFLAGS="-arch $arch -mmacosx-version-min=$min_version" > "$log" 2>&1; then
        cat "$log"
        echo "Build for $arch failed"
        exit 1
    fi

    cp -L "$WORK_DIR/zstd/lib/libzstd.dylib" "$OUT_DIR/$name"
    echo "== $OUT_DIR/$name"
    lipo -info "$OUT_DIR/$name"
    otool -L "$OUT_DIR/$name"
}

build x86_64 10.15 libzstd.mac.x64.dylib
build arm64 11.0 libzstd.mac.arm64.dylib

echo "Built zstd $ZSTD_VERSION"
