param(
    $Repo = "ravendb/ravendb",
    $DockerSettingsFile = "src\Raven.Server\Properties\Settings\settings.docker.posix.json")

$ErrorActionPreference = "Stop"

function BuildUbuntuDockerImage ($projectDir, $version, $repo, $settingsFile) {
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

    write-host "Build docker image: $version"
    write-host "Tags: $($repo):$version-ubuntu.16.04-x64 $($repo):latest $($repo):ubuntu-latest"

    docker build ./ravendb-ubuntu1604 `
        -t "$($repo):$version-ubuntu.16.04-x64" `
        -t "$($repo):latest" `
        -t "$($repo):ubuntu-latest"

    Remove-Item "./ravendb-ubuntu1604/RavenDB.tar.bz2"
}

function GetVersionFromArtifactName() {
    $versionRegex = [regex]'RavenDB-([0-9]\.[0-9]\.[0-9](-[a-zA-Z]+-[0-9-]+)?)-[a-z]+'
    $fname = $(Get-ChildItem "../artifacts" `
        | Where-Object { $_.Name -Match $versionRegex } `
        | Sort-Object LastWriteTime -Descending `
        | Select-Object -First 1).Name
    $match = $fname | select-string -Pattern $versionRegex
    $version = $match.Matches[0].Groups[1]

    if (!$version) {
        throw "Could not parse version from artifact file name: $fname"
    }

    return $version
}

BuildUbuntuDockerImage ".." $(GetVersionFromArtifactName) $Repo $DockerSettingsFile
