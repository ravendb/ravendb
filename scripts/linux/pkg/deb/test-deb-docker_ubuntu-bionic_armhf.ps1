$env:DISTRO_NAME = "ubuntu"
$env:DISTRO_VERSION = "18.04"
$env:DISTRO_VERSION_NAME ="bionic"

$env:OUTPUT_DIR = "$PSScriptRoot/dist"

.\set-raven-platform-armhf.ps1
.\set-raven-version-env.ps1

.\test-deb-docker.ps1
