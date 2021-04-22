$env:DISTRO_NAME = "ubuntu"
$env:DISTRO_VERSION = "20.04"
$env:DISTRO_VERSION_NAME ="focal"

$env:RAVEN_PLATFORM="linux-x64"
$env:DOCKER_BUILDPLATFORM = "linux/amd64"
$env:DEB_ARCHITECTURE="amd64"

$env:OUTPUT_DIR = "$PSScriptRoot/dist"

$env:RAVENDB_VERSION = "4.2.112"

.\build-deb.ps1
