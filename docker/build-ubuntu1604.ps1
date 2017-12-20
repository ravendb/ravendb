param(
    $Version = "4.0.0-custom-40")

$ErrorActionPreference = "Stop"

function BuildUbuntuDockerImage ( $projectDir, $version = "4.0.0-custom-40" ) {
    $packageFileName = "RavenDB-$version-linux-x64.tar.bz2"
    $packagePath = [io.path]::combine($projectDir, "artifacts", $packageFileName)

    if ([string]::IsNullOrEmpty($packagePath))
    {
        throw "PackagePath cannot be empty."
    }

    if ($(Test-Path $packagePath) -eq $False) {
        throw "Package file does not exist."
    }
    
    Copy-Item -Destination ./ravendb-ubuntu1604/RavenDB.tar.bz2 -Force -Path $packagePath

    $settingsPath = Join-Path -Path $projectDir -ChildPath "src\Raven.Server\Properties\Settings\settings.docker.posix.json"
    Copy-Item -Path $settingsPath -Destination ./ravendb-ubuntu1604/settings.json -Force

    docker build ./ravendb-ubuntu1604 `
        -t ravendb/ravendb:$version-ubuntu.16.04-x64 `
        -t ravendb/ravendb:latest `
        -t ravendb/ravendb:ubuntu-latest

    Remove-Item "./ravendb-ubuntu1604/RavenDB.tar.bz2"
}

BuildUbuntuDockerImage ".." $Version
