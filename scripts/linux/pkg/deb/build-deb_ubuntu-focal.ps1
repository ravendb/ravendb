$env:DISTRO_NAME = "ubuntu"
$env:DISTRO_VERSION = "20.04"
$env:DISTRO_VERSION_NAME ="focal"

$env:OUTPUT_DIR = "$PSScriptRoot/dist"

$env:RAVENDB_VERSION = "4.2.111"
$env:DOTNET_RUNTIME_VERSION = "3.1" 
$env:DOTNET_DEPS_VERSION = "3.1.12"
$env:DEB_DOTNET_RUNTIME_DEPS_LINE = "dotnet-runtime-deps-$($env:DOTNET_RUNTIME_VERSION) (>= $env:DOTNET_DEPS_VERSION)"

.\build-deb.ps1
