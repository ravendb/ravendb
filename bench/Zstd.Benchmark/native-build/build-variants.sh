#!/bin/bash
# Builds libzstd variants with the production build functions (src/Raven.Pal/build-libs/zstd.sh).
#
# Mounts expected by this script:
#   /scripts   src/Raven.Pal/build-libs (read-only)
#   /zstd-src  a zstd git repository holding the refs to build (read-only)
#   /out       output; every variant lands in /out/<name>/
#
# Usage: build-variants.sh <name>=<git ref>[:<targets>] ...
#   targets: comma separated subset of linux64,win64,win32,arm64,arm32 (default: all of them)
#   e.g. build-variants.sh 1.5.7=v1.5.7 1.5.7-Os=v1.5.7:win64os   (refs of a facebook/zstd clone)

set -e

# zstd.sh sources its dependencies relative to the current directory
pushd /scripts > /dev/null
source ./colors.sh
source ./zstd.sh
popd > /dev/null

# x64 build with the flags the arm targets use (-Os), to estimate what -Os costs
function zstd_lib_win64os {
    zstd_lib_release win64os \
        make lib-release \
            OS=Windows_NT \
            CC=x86_64-w64-mingw32-gcc \
            CFLAGS="-Os -m64" && \
        cp zstd/lib/dll/libzstd.dll "${ARTIFACTS_DIR}/libzstd.win.x64.dll" >> ${ZSTD_LOG} 2>&1
}

for variant in "$@"; do
    name="${variant%%=*}"
    spec="${variant#*=}"
    ref="${spec%%:*}"
    targets="linux64,win64,win32,arm64,arm32"
    if [[ "$spec" == *:* ]]; then
        targets="${spec#*:}"
    fi

    export ARTIFACTS_DIR="/out/${name}"
    export ZSTD_LOG="/out/${name}/build.log"
    mkdir -p "$ARTIFACTS_DIR"
    : > "$ZSTD_LOG"

    echo "=== ${name}: ${ref} (${targets})"
    rm -rf /build/zstd
    git clone --quiet /zstd-src /build/zstd
    git -C /build/zstd checkout --quiet "$ref"
    git -C /build/zstd log -1 --format='%H %s' > "${ARTIFACTS_DIR}/source.txt"

    cd /build
    for target in ${targets//,/ }; do
        "zstd_lib_${target}" || { echo "build failed: ${name} ${target}, see ${ZSTD_LOG}"; exit 1; }
    done

    # record what each binary links against: shipped binaries must not depend on mingw/gcc runtime DLLs
    for f in "${ARTIFACTS_DIR}"/*.dll; do
        [ -e "$f" ] || continue
        echo "--- $(basename "$f")" >> "${ARTIFACTS_DIR}/dependencies.txt"
        x86_64-w64-mingw32-objdump -p "$f" | grep "DLL Name" >> "${ARTIFACTS_DIR}/dependencies.txt" || true
    done
    for f in "${ARTIFACTS_DIR}"/*.so; do
        [ -e "$f" ] || continue
        echo "--- $(basename "$f")" >> "${ARTIFACTS_DIR}/dependencies.txt"
        objdump -p "$f" | grep "NEEDED" >> "${ARTIFACTS_DIR}/dependencies.txt" || true
    done
done
