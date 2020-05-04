#!/bin/bash

export ZSTD_LOG=${PWD}/build_zstd.log

LIBZSTD_VER="ravendb"
LIBZSTD_REPO="https://github.com/ravendb/zstd.git"

function zstd_install_build_deps {
    pushd zstd >> ${ZSTD_LOG} 2>&1
    echo -ne "${C_YELLOW}[`date`] ${C_L_MAGENTA}Install zstd build dependencies... ${NC}" >> ${ZSTD_LOG} 2>&1 
    make arminstall >> ${ZSTD_LOG} 2>&1 || return 1
    echo -e "${C_GREEN}ok." 
    popd >> ${ZSTD_LOG} 2>&1
    return 0
}

function zstd_clone {
    echo -ne "${C_YELLOW}[`date`] ${C_L_MAGENTA}Cloning zstd... ${NC}"
    git clone --branch $LIBZSTD_VER --single-branch "$LIBZSTD_REPO" 
    if [ $? -ne 0 ]; then
            echo -e "${ERR_STRING}Failed to clone zstd. This is a fatal error for the build process${NC}"
            exit 1
    fi		
    echo -e "${C_GREEN}ok."
    pushd zstd >> ${ZSTD_LOG} 2>&1
    if [ $? -ne 0 ]; then echo -e "${ERR_STRING}Failed to pushd zstd. See ${ZSTD_LOG} for details.${NC}"; popd; exit 1; fi
    echo -ne "${C_YELLOW}[`date`] ${C_L_MAGENTA}Checkout ${C_L_CYAN}${LIBZSTD_VER}${C_L_MAGENTA}... ${NC}"
    git checkout ${LIBZSTD_VER} >> ${ZSTD_LOG} 2>&1
    if [ $? -ne 0 ]; then echo -e "${ERR_STRING}Failed to checkout tags/${LIBZSTD_VER}. See ${ZSTD_LOG} for details.${NC}"; popd; exit 1; fi
    echo -e "${C_GREEN}ok."
    git show
    popd >> ${ZSTD_LOG} 2>&1
}

function zstd_build_reset {
    unset CC
    unset CFLAGS
    unset PREFIX
    unset OSX_VERSION_MIN
    unset OSX_CPU_ARCH
    unset LDFLAGS
    unset UNAME

    if [ ! -d zstd ]; then
        zstd_clone
    fi

    if [ ! -d "$ARTIFACTS_DIR" ]; then
        mkdir -p "$ARTIFACTS_DIR"
    fi

    pushd zstd >> ${ZSTD_LOG} 2>&1

    echo -ne "${C_YELLOW}[`date`] ${C_L_MAGENTA}Cleaning... "
    git clean -fxd >> ${ZSTD_LOG} 2>&1
    echo -e "${C_GREEN}ok."

    popd >> ${ZSTD_LOG} 2>&1
}
