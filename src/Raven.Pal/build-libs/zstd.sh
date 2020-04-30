#!/bin/bash

source "./zstd-build-deps.sh"

function zstd_lib_release {
    local target=$1
    shift
    local ZSTD_LOG=${PWD}/build_zstd_$target.log

    zstd_build_reset

    pushd zstd >> ${ZSTD_LOG} 2>&1
    make clean >> ${ZSTD_LOG} 2>&1 
    
    echo -ne "${C_YELLOW}[`date`] ${C_L_MAGENTA}Building for ${C_L_BLUE}$target${C_L_MAGENTA}... "
    # make call
    "$@" >> ${ZSTD_LOG} 2>&1

    if [ $? -ne 0 ]; then 
        echo -e "${ERR_STRING}Failed. See ${ZSTD_LOG} for details.${NC}";
        popd >> ${ZSTD_LOG} 2>&1
        return 1
    fi

    popd >> ${ZSTD_LOG} 2>&1
    echo -e "${C_GREEN}ok."
    return 0
}

function zstd_lib_arm32 {
    zstd_lib_release arm32 \
        make lib-release CC=arm-linux-gnueabihf-gcc CFLAGS="-Werror -Os" && \
        cp zstd/lib/libzstd.so "${ARTIFACTS_DIR}/libzstd.arm.32.so" >> ${ZSTD_LOG} 2>&1
}


function zstd_lib_arm64 {
    zstd_lib_release arm64 \
        make lib-release CC=aarch64-linux-gnu-gcc CFLAGS="-Werror -Os" && \
        cp zstd/lib/libzstd.so "${ARTIFACTS_DIR}/libzstd.arm.64.so" >> ${ZSTD_LOG} 2>&1
}

function zstd_lib_win32 {
    zstd_lib_release win32 \
        make lib-release \
            CC=i686-w64-mingw32-gcc \
            OS=Windows_NT \
            CFLAGS="-Ofast -fomit-frame-pointer -m32 -march=pentium3 -mtune=westmere" && \
        cp zstd/lib/dll/libzstd.dll "${ARTIFACTS_DIR}/libzstd.win.32.dll" >> ${ZSTD_LOG} 2>&1
}

function zstd_lib_win64 {
    zstd_lib_release win64 \
        make lib-release \
            OS=Windows_NT \
            CC=x86_64-w64-mingw32-gcc \
            CFLAGS="-Ofast -fomit-frame-pointer -m64 -mtune=westmere" && \
        cp zstd/lib/dll/libzstd.dll "${ARTIFACTS_DIR}/libzstd.win.64.dll" >> ${ZSTD_LOG} 2>&1
}

function zstd_lib_linux64 {
    zstd_lib_release linux64 \
        make lib-release && \
        cp zstd/lib/libzstd.so "${ARTIFACTS_DIR}/libzstd.linux.x64.so" >> ${ZSTD_LOG} 2>&1
}

function zstd_lib_osx {
	export OSX_VERSION_MIN=${OSX_VERSION_MIN-"10.8"}
	export OSX_CPU_ARCH=${OSX_CPU_ARCH-"core2"}

    zstd_lib_release osx \
        make lib-release \
            V=1 \
            UNAME="Darwin" \
            CC="/osxcross/target/bin/o64-clang" \
            CFLAGS="-arch x86_64 -mmacosx-version-min=${OSX_VERSION_MIN} -march=${OSX_CPU_ARCH} -O2 -g" \
            LDFLAGS="-arch x86_64 -mmacosx-version-min=${OSX_VERSION_MIN} -march=${OSX_CPU_ARCH}" && \
        cp zstd/lib/libzstd.dylib "${ARTIFACTS_DIR}/libzstd.mac.64.dylib"
}

function zstd_cross_build {
    echo -e "${C_L_GREEN}${T_BOLD}Build zstd${NC}${NT}"
    echo -e "${C_D_GRAY}===============${NC}"
    echo ""

    sum=0
    for target in linux64 arm32 arm64 win32 win64 osx
    do
        "zstd_lib_$target"
        sum=$(( $sum + 1 ))
    done

    if [[ $sum != 6 ]]; then
        echo -ne "${C_YELLOW}[`date`] ${C_L_RED}Some builds failed...${NC}"
        return 1
    fi

    echo -e "${C_YELLOW}[`date`] ${C_L_GREEN}All builds succeeded.${NC}"
    echo -e "${C_YELLOW}[`date`] ${C_L_GREEN}Artifacts:${NC}"
    ls ${ARTIFACTS_DIR}
    
    return 0
}
