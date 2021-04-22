$env:DISTRO_NAME = "ubuntu"
$env:DISTRO_VERSION = "18.04"
$env:DISTRO_VERSION_NAME ="bionic"

$env:OUTPUT_DIR = "$PSScriptRoot/dist"

$env:RAVENDB_VERSION = "4.2.112"

./test-deb-vagrant.ps1
