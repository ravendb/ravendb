param(
    $Repo = "ravendb/ravendb",
    $ArtifactsDir = "..\artifacts",
    $RavenDockerSettingsPath = "..\src\Raven.Server\Properties\Settings\settings.docker.posix.json",
    $Arch = "x64",
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
            throw "Arch not supported (currently x64 and arm32v7 are supported)"
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
    $tags = GetUbuntuImageTags $repo $version $arch
    write-host "Tags: $tags"

    $fullNameTag = $tags[0]

    docker build $DockerfileDir -f "$($DockerfileDir)/Dockerfile.$($arch)" -t "$fullNameTag"
    CheckLastExitCode
    
    foreach ($tag in $tags[1..$tags.Length]) {
        write-host "Tag $fullNameTag as $tag"
        docker tag "$fullNameTag" $tag
        CheckLastExitCode
    }

    Remove-Item -Path $dockerPackagePath
}

BuildUbuntuDockerImage $(GetVersionFromArtifactName) $Arch
