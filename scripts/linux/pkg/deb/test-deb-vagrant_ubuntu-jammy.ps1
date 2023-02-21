$env:DISTRO_NAME = "ubuntu"
$env:DISTRO_VERSION = "22.04"
$env:DISTRO_VERSION_NAME ="jammy"

$env:OUTPUT_DIR = "$PSScriptRoot/dist"

./set-raven-version-env.ps1

./test-deb-vagrant.ps1
