param(
    $Version = "4.0.0-custom-40")

$ErrorActionPreference = "Stop"

function BuildWindowsDockerImage ( $projectDir, $version = "4.0.0-custom-40" ) {
    $packageFileName = "RavenDB-$version-windows-x64.zip"
    $packagePath = [io.path]::combine($projectDir, "artifacts", $packageFileName)

    if ([string]::IsNullOrEmpty($packagePath))
    {
        throw "PackagePath cannot be empty."
    }

    if ($(Test-Path $packagePath) -eq $False) {
        throw "Package file does not exist."
    }

    Copy-Item -Destination ./ravendb-nanoserver -Force $packagePath

    docker build ./ravendb-nanoserver `
        -t ravendb/ravendb:$version-windows-nanoserver `
        -t ravendb/ravendb:windows-nanoserver-latest

    Remove-Item "./ravendb-nanoserver/$packageFileName"
}

BuildWindowsDockerImage ".." $Version
