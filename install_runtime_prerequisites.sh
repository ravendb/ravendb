#!/bin/bash

UBUNTU_CODENAME=$(lsb_release -c 2>/dev/null | cut -d ":" -f2 | sed 's/\t//g')
UBUNTU_VERSION=$(lsb_release -r 2>/dev/null | cut -d ":" -f2 | sed 's/\t//g')

# Fallback to /etc/os-release if lsb_release is not available (e.g. minimal Docker images)
if [ -z "$UBUNTU_CODENAME" ] || [ -z "$UBUNTU_VERSION" ]; then
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        if [ -z "$UBUNTU_CODENAME" ] && [ -n "$VERSION_CODENAME" ]; then
            UBUNTU_CODENAME="$VERSION_CODENAME"
        fi
        if [ -z "$UBUNTU_VERSION" ] && [ -n "$VERSION_ID" ]; then
            UBUNTU_VERSION="$VERSION_ID"
        fi
    fi
fi

# Determine how to run privileged commands
APT_PREFIX=""
if [ "$EUID" -eq 0 ]; then
    APT_PREFIX=""
elif command -v sudo &> /dev/null; then
    APT_PREFIX="sudo"
else
    echo "Note: Not running as root and sudo is not available."
    echo "Will attempt to check for existing libraries. If packages are missing, install them manually."
    echo ""
fi

INSTALL_FAILURES=0

apt_install() {
    if [ -n "$APT_PREFIX" ] || [ "$EUID" -eq 0 ]; then
        if ! $APT_PREFIX apt-get install -y --no-install-recommends "$@"; then
            INSTALL_FAILURES=$((INSTALL_FAILURES + 1))
            return 1
        fi
        return 0
    else
        # Check if all requested packages are already installed
        local missing=0
        for pkg in "$@"; do
            if ! dpkg -s "$pkg" &>/dev/null; then
                missing=1
                break
            fi
        done
        if [ "$missing" -eq 0 ]; then
            echo "Already installed: $*"
            return 0
        fi
        echo "WARNING: Cannot install $* via apt-get (no root/sudo). Please install manually."
        INSTALL_FAILURES=$((INSTALL_FAILURES + 1))
        return 1
    fi
}

# install .NET runtime dependencies
echo "Installing .NET runtime dependencies..."

# libcurl3 was replaced by libcurl4 in Ubuntu 18.04+
if [ -n "$UBUNTU_VERSION" ] && dpkg --compare-versions "$UBUNTU_VERSION" "lt" "18.04" 2>/dev/null; then
    LIBCURL_PKG="libcurl3"
else
    LIBCURL_PKG="libcurl4"
fi

apt_install libunwind8 ca-certificates "$LIBCURL_PKG"

if [ "$UBUNTU_VERSION" = "14.04" ] ; then
    apt_install libicu52
elif [ "$UBUNTU_VERSION" = "16.04" ] ; then
    apt_install libicu55
elif [ "$UBUNTU_VERSION" = "18.04" ] ; then
    apt_install libicu60
elif [ "$UBUNTU_VERSION" = "20.04" ] ; then
    apt_install libicu66
elif [ "$UBUNTU_VERSION" = "22.04" ] ; then
    apt_install libicu70
elif [ "$UBUNTU_VERSION" = "24.04" ] ; then
    apt_install libicu74
elif [ "$UBUNTU_VERSION" = "26.04" ] ; then
    apt_install libicu76
fi

if [ "$INSTALL_FAILURES" -gt 0 ]; then
    echo ""
    echo "WARNING: $INSTALL_FAILURES package(s) could not be installed (no root/sudo)."
    echo "Please install the missing packages manually before running RavenDB."
    exit 1
fi

echo "Runtime prerequisites setup complete."
