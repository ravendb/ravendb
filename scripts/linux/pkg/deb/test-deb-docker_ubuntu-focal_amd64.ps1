$env:DISTRO_NAME = "ubuntu"
$env:DISTRO_VERSION = "20.04"
$env:DISTRO_VERSION_NAME ="focal"

$env:OUTPUT_DIR = "$PSScriptRoot/dist"

.\set-raven-platform-amd64.ps1
.\set-raven-version-env.ps1

.\test-deb-docker.ps1
