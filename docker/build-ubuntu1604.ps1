param(
    $Version = "4.0.0-custom-40")

$ErrorActionPreference = "Stop"

function BuildUbuntuDockerImage ( $projectDir, $version = "4.0.0-custom-40" ) {
    $packageFileName = "RavenDB-$version-ubuntu.16.04-x64.tar.bz2"
    $packagePath = [io.path]::combine($projectDir, "artifacts", $packageFileName)

    if ([string]::IsNullOrEmpty($packagePath))
    {
        throw "PackagePath cannot be empty."
    }

    if ($(Test-Path $packagePath) -eq $False) {
        throw "Package file does not exist."
    }

    Copy-Item -Destination ./ravendb-ubuntu1604 -Force $packagePath

    docker build ./ravendb-ubuntu1604 `
        -t ravendb/ravendb:$version-ubuntu.16.04-x64 `
        -t ravendb/ravendb:latest `
        -t ravendb/ravendb:ubuntu-latest

    Remove-Item "./ravendb-ubuntu1604/$packageFileName"
}

BuildUbuntuDockerImage ".." $Version
