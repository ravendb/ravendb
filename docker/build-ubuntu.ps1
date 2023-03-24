param(
    $Repo = "ravendb/ravendb",
    $ArtifactsDir = "..\artifacts",
    $RavenDockerSettingsPath = "..\src\Raven.Server\Properties\Settings\settings.docker.posix.json",
    $Arch = "x64",
    $DockerfileDir = "./ravendb-ubuntu",
    $DebPackagePath="")

$ErrorActionPreference = "Stop"

. ".\common.ps1"

function GetPackageFileName($arch)
{
    switch ($arch) {
        "arm32v7" { 
            return "RavenDB-$version-raspberry-pi.tar.bz2"
        }
        "arm64v8" {
            return "RavenDB-$version-linux-arm64.tar.bz2"
        }
        "x64" {
            return "RavenDB-$version-linux-x64.tar.bz2"
        }
        Default {
            throw "Arch not supported (currently x64 and arm32v7 are supported)"
        }
    }
}

function GetUbuntuVersionFromDockerfile($DockerfileDir, $DockerfileName) {
    $dockerfilePath = Join-Path $DockerfileDir "Dockerfile.$arch"
    $dockerfileContents = Get-Content $dockerfilePath
    $ubuntuVersion = $dockerfileContents | Select-String -Pattern "(?<=FROM\s+mcr\.microsoft\.com\/dotnet\/runtime-deps:7\.0-)(.*)"
    return $ubuntuVersion.Matches.Groups[1].Value
}

function GetBuildScriptNameMatchingArchOSVer($arch, $ubuntu_version){
    switch ($arch) {
        "x64" {
            switch ($ubuntu_version) {
                "bionic" {
                    $build_script = "build-deb_ubuntu-bionic_amd64.ps1"
                }
                "focal" {
                    $build_script = "build-deb_ubuntu-focal_amd64.ps1"
                }
                "jammy" {
                    $build_script = "build-deb_ubuntu-jammy_amd64.ps1"
                }
                Default {
                    Write-Error "ERROR: Unexpected Ubuntu version $($ubuntu_version). Supported versions: bionic, focal, jammy."
                    exit 1
                }
            }
        }
        "arm64v8" {
            switch ($ubuntu_version) {
                "bionic" {
                    $build_script = "build-deb_ubuntu-bionic_arm64.ps1"
                }
                "focal" {
                    $build_script = "build-deb_ubuntu-focal_arm64.ps1"
                }
                "jammy" {
                    $build_script = "build-deb_ubuntu-jammy_arm64.ps1"
                }
                Default {
                    Write-Error "ERROR: Unsupported Ubuntu version $($ubuntu_version) for ARM64v8 architecture. Supported version: bionic, focal, jammy."
                    exit 1
                }
            }
        }
        "arm32v7" {
            switch ($ubuntu_version) {
                "bionic" {
                    $build_script = "build-deb_ubuntu-bionic_armhf.sh"
                }
                "focal" {
                    $build_script = "build-deb_ubuntu-focal_armhf.sh"
                }
                "jammy" {
                    $build_script = "build-deb_ubuntu-jammy_armhf.sh"
                }
                Default {
                    Write-Error "ERROR: Unsupported Ubuntu version $($ubuntu_version) for ARM32v7 architecture. Supported version: bionic, focal, jammy."
                    exit 1
                }
            }
        }
        Default {
            Write-Error "ERROR: Unsupported architecture $($arch)"
            exit 1
        }
    }
}


function BuildUbuntuDockerImage ($version, $arch) {
    $packageFileName = GetPackageFileName $arch
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

    if ([string]::IsNullOrEmpty($DebPackagePath)) {
        $ubuntu_version = GetUbuntuVersionFromDockerfile $DockerfileDir "Dockerfile.$($arch)"
        $build_script_name = GetBuildScriptNameMatchingArchOSVer $arch $ubuntu_version
        $build_script_full_path = Join-Path "..\scripts\linux\pkg\deb\" $build_script_name

        $path_to_deb = Join-Path $DockerfileDir "ravendb-$($arch)-$($ubuntu_version).deb"
        $env:OUTPUT_DIR = $path_to_deb
        $build_script_full_path 
    }
    else {
        $path_to_deb="$DebPackagePath"
    }

    docker build $DockerfileDir -f "$($DockerfileDir)/Dockerfile.$($arch)" -t "$fullNameTag" --build-arg path_to_deb="$path_to_deb"
    CheckLastExitCode
    
    foreach ($tag in $tags[1..$tags.Length]) {
        write-host "Tag $fullNameTag as $tag"
        docker tag "$fullNameTag" $tag
        CheckLastExitCode
    }

    Remove-Item -Path $dockerPackagePath
}

BuildUbuntuDockerImage $(GetVersionFromArtifactName) $Arch
