param(
    $Repo = "ravendb/ravendb",
    $ArtifactsDir = "..\artifacts",
    $RavenDockerSettingsPath = "..\src\Raven.Server\Properties\Settings\settings.docker.posix.json",
    $DockerfileDir = "./ravendb-ubuntu")

$ErrorActionPreference = "Stop"

. ".\common.ps1"

function BuildUbuntuDockerImage ($version, $arch) {
    switch ($arch) {
        "arm32v7" { 
            $packageFileName = "RavenDB-$version-raspberry-pi.tar.bz2"
            break;
        }
        "x64" {
            $packageFileName = "RavenDB-$version-linux-x64.tar.bz2"
            break;
        }
        Default {
            throw "Arch not supported."
        }
    }

    $artifactsPackagePath = Join-Path -Path $ArtifactsDir -ChildPath $packageFileName

    if ([string]::IsNullOrEmpty($artifactsPackagePath)) {
        throw "PackagePath cannot be empty."
    }

    if ($(Test-Path $artifactsPackagePath) -eq $False) {
        throw "Package file does not exist."
    }
    
    $dockerPackagePath = Join-Path -Path $DockerfileDir -ChildPath "RavenDB.tar.bz2"
    Copy-Item -Path $artifactsPackagePath -Destination $dockerPackagePath -Force
    Copy-Item -Path $RavenDockerSettingsPath -Destination $(Join-Path -Path $DockerfileDir -ChildPath "settings.json") -Force

    write-host "Build docker image: $version"
    write-host "Tags: $($repo):$version-ubuntu.18.04-$arch $($repo):4.2-ubuntu-$arch-latest"

    docker build $DockerfileDir `
        -f "$($DockerfileDir)/Dockerfile.$($arch)" `
        -t "$($repo):latest" `
        -t "$($repo):ubuntu-$arch-latest" `
        -t "$($repo):$version-ubuntu.18.04-$arch" `
        -t "$($repo):4.2-ubuntu-$arch-latest"

    Remove-Item -Path $dockerPackagePath
}

BuildUbuntuDockerImage $(GetVersionFromArtifactName) -arch "x64"
BuildUbuntuDockerImage $(GetVersionFromArtifactName) -arch "arm32v7"
