$env:DISTRO_NAME = "ubuntu"
$env:DISTRO_VERSION = "18.04"
$env:DISTRO_VERSION_NAME ="bionic"

$env:RAVEN_PLATFORM="raspberry-pi"
$env:DOCKER_BUILDPLATFORM = "linux/arm/v7"
$env:DEB_ARCHITECTURE="armhf"

$env:OUTPUT_DIR = "$PSScriptRoot/dist"

$env:RAVENDB_VERSION = "4.2.112"

./test-deb-docker.ps1
