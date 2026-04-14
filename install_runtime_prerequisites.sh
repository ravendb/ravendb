#!/bin/bash

UBUNTU_CODENAME=$(lsb_release -c 2>/dev/null | cut -d ":" -f2 | sed 's/\t//g')
UBUNTU_VERSION=$(lsb_release -r 2>/dev/null | cut -d ":" -f2 | sed 's/\t//g')

# Determine how to run privileged commands
APT_PREFIX=""
if [ "$EUID" -eq 0 ]; then
    APT_PREFIX=""
elif command -v sudo &> /dev/null && sudo -n true 2>/dev/null; then
    APT_PREFIX="sudo"
else
    echo "Note: Not running as root and sudo is not available."
    echo "Will attempt to check for existing libraries. If packages are missing, install them manually."
    echo ""
fi

apt_install() {
    if [ -n "$APT_PREFIX" ] || [ "$EUID" -eq 0 ]; then
        $APT_PREFIX apt-get install -y --no-install-recommends "$@"
    else
        echo "WARNING: Cannot install $* via apt-get (no root/sudo). Please install manually."
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
fi

echo "Runtime prerequisites setup complete."
