$ErrorActionPreference = "Stop"

function BuildUbuntuDockerImage ( $projectDir, $version) {
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

$versionRegex = [regex]'RavenDB-([0-9]\.[0-9]\.[0-9](-[a-zA-Z]+-[0-9-]+)?)-[a-z]+'
$fname = $(Get-ChildItem "../artifacts" | Where-Object { $_.Name -Match $versionRegex } | Select-Object -First 1).Name
$match = $fname | select-string -Pattern $versionRegex
$version = $match.Matches[0].Groups[1]

BuildUbuntuDockerImage ".." $version 
