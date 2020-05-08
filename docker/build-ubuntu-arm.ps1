param(
    $Repo = "ravendb/ravendb",
    $ArtifactsDir = "..\artifacts",
    $RavenDockerSettingsPath = "..\src\Raven.Server\Properties\Settings\settings.docker.posix.json",
    $DockerfileDir = "./ravendb-ubuntu-arm")

$ErrorActionPreference = "Stop"

. ".\common.ps1"

function BuildUbuntuArmDockerImage ($version) {
    $packageFileName = "RavenDB-$version-raspberry-pi.tar.bz2"
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
    write-host "Tags: $($repo):$version-ubuntu.18.04-arm $($repo):4.2-ubuntu-arm-latest"

    docker build $DockerfileDir `
        -t "$($repo):latest" `
        -t "$($repo):ubuntu-arm-latest" `
        -t "$($repo):$version-ubuntu.18.04-arm" `
        -t "$($repo):4.2-ubuntu-arm-latest"

    Remove-Item -Path $dockerPackagePath
}

BuildUbuntuArmDockerImage $(GetVersionFromArtifactName)
