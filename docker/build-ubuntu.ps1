param(
    $Repo = "ravendb/ravendb",
    $ArtifactsDir = "..\artifacts",
    $RavenDockerSettingsPath = "..\src\Raven.Server\Properties\Settings\settings.docker.posix.json",
    $DockerfileDir = "./ravendb-ubuntu")

$ErrorActionPreference = "Stop"

function BuildUbuntuDockerImage ($version) {
    $packageFileName = "RavenDB-$version-linux-x64.tar.bz2"
    $artifactsPackagePath = Join-Path -Path $ArtifactsDir -ChildPath $packageFileName

    if ([string]::IsNullOrEmpty($artifactsPackagePath))
    {
        throw "PackagePath cannot be empty."
    }

    if ($(Test-Path $artifactsPackagePath) -eq $False) {
        throw "Package file does not exist."
    }
    
    $dockerPackagePath = Join-Path -Path $DockerfileDir -ChildPath "RavenDB.tar.bz2"
    Copy-Item -Path $artifactsPackagePath -Destination $dockerPackagePath -Force
    Copy-Item -Path $RavenDockerSettingsPath -Destination $(Join-Path -Path $DockerfileDir -ChildPath "settings.json") -Force

    write-host "Build docker image: $version"
    write-host "Tags: $($repo):$version-ubuntu.18.04-x64 $($repo):4.2-ubuntu-latest"

    docker build $DockerfileDir `
        -t "$($repo):latest" `
        -t "$($repo):ubuntu-latest" `
        -t "$($repo):$version-ubuntu.18.04-x64" `
        -t "$($repo):4.2-ubuntu-latest"

    Remove-Item -Path $dockerPackagePath
}

function GetVersionFromArtifactName() {
    $versionRegex = [regex]'RavenDB-([0-9]\.[0-9]\.[0-9](-[a-zA-Z]+-[0-9-]+)?)-[a-z]+'
    $fname = $(Get-ChildItem $ArtifactsDir `
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

BuildUbuntuDockerImage $(GetVersionFromArtifactName)
