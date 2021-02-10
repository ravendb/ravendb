$env:DISTRO_NAME = "ubuntu"
$env:DISTRO_VERSION = "20.04"
$env:DISTRO_VERSION_NAME ="focal"

$env:OUTPUT_DIR = "$PSScriptRoot/dist"

$env:RAVENDB_VERSION = "4.2.112"
$env:DOTNET_RUNTIME_VERSION = "3.1" 
$env:DOTNET_DEPS_VERSION = "3.1.12"
./test-deb-vagrant.ps1
