param(
    $Repo = "ravendb/ravendb",
    $ArtifactsDir = "..\artifacts",
    $RavenDockerSettingsPath = "..\src\Raven.Server\Properties\Settings\settings.docker.posix.json",
    $DockerfileDir = "./ravendb-ubuntu")

$ErrorActionPreference = "Stop"

. ".\common.ps1"

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
    write-host "Tags: $($repo):$version-ubuntu.18.04-x64 $($repo):4.1-ubuntu-latest"

    docker build $DockerfileDir `
        -t "$($repo):latest" `
        -t "$($repo):ubuntu-latest" `
        -t "$($repo):$version-ubuntu.18.04-x64" `
        -t "$($repo):4.1-ubuntu-latest"

    Remove-Item -Path $dockerPackagePath
}

BuildUbuntuDockerImage $(GetVersionFromArtifactName)
