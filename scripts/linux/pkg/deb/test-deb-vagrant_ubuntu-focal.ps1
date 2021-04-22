$env:DISTRO_NAME = "ubuntu"
$env:DISTRO_VERSION = "20.04"
$env:DISTRO_VERSION_NAME ="focal"

$env:OUTPUT_DIR = "$PSScriptRoot/dist"

$env:RAVENDB_VERSION = "4.2.112"

./test-deb-vagrant.ps1
