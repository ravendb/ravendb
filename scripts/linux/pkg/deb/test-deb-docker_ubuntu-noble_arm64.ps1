$env:DISTRO_NAME = "ubuntu"
$env:DISTRO_VERSION = "24.04"
$env:DISTRO_VERSION_NAME ="noble"

$env:OUTPUT_DIR = "$PSScriptRoot/dist"

.\set-raven-platform-arm64.ps1
.\set-raven-version-env.ps1

.\test-deb-docker.ps1
