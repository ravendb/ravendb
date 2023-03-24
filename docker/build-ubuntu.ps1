param(
    $Repo = "ravendb/ravendb",
    $ArtifactsDir = "..\artifacts",
    $RavenDockerSettingsPath = "..\src\Raven.Server\Properties\Settings\settings.docker.posix.json",
    $Arch = "x64",
    $DockerfileDir = "./ravendb-ubuntu",
    $DebPackagePath="",
    [switch]$NoCache)

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

function GetBuildScriptNameMatchingArchOSVer($arch, $ubuntuVersion){
    switch ($arch) {
        "x64" {
            switch ($ubuntuVersion) {
                "bionic" {
                    $buildScriptFileName = "build-deb_ubuntu-bionic_amd64.ps1"
                }
                "focal" {
                    $buildScriptFileName = "build-deb_ubuntu-focal_amd64.ps1"
                }
                "jammy" {
                    $buildScriptFileName = "build-deb_ubuntu-jammy_amd64.ps1"
                }
                Default {
                    Write-Error "ERROR: Unexpected Ubuntu version $($ubuntuVersion). Supported versions: bionic, focal, jammy."
                    exit 1
                }
            }
        }
        "arm64v8" {
            switch ($ubuntuVersion) {
                "bionic" {
                    $buildScriptFileName = "build-deb_ubuntu-bionic_arm64.ps1"
                }
                "focal" {
                    $buildScriptFileName = "build-deb_ubuntu-focal_arm64.ps1"
                }
                "jammy" {
                    $buildScriptFileName = "build-deb_ubuntu-jammy_arm64.ps1"
                }
                Default {
                    Write-Error "ERROR: Unsupported Ubuntu version $($ubuntuVersion) for ARM64v8 architecture. Supported version: bionic, focal, jammy."
                    exit 1
                }
            }
        }
        "arm32v7" {
            switch ($ubuntuVersion) {
                "bionic" {
                    $buildScriptFileName = "build-deb_ubuntu-bionic_armhf.sh"
                }
                "focal" {
                    $buildScriptFileName = "build-deb_ubuntu-focal_armhf.sh"
                }
                "jammy" {
                    $buildScriptFileName = "build-deb_ubuntu-jammy_armhf.sh"
                }
                Default {
                    Write-Error "ERROR: Unsupported Ubuntu version $($ubuntuVersion) for ARM32v7 architecture. Supported version: bionic, focal, jammy."
                    exit 1
                }
            }
        }
        Default {
            Write-Error "ERROR: Unsupported architecture $($arch)"
            exit 1
        }
    }
    return $buildScriptFileName
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
        $ubuntuVersion = GetUbuntuVersionFromDockerfile $DockerfileDir "Dockerfile.$arch"
        $buildScriptFileName = GetBuildScriptNameMatchingArchOSVer $arch $ubuntuVersion
        $buildScriptPath = (Resolve-Path $(Join-Path "..\scripts\linux\pkg\deb\" $buildScriptFileName)).Path
        
        $archNameToMatch = switch ($arch) {
            "x64" { "amd64"; break }
            "arm32v7" { "arm32"; break }
            "arm64v8" { "arm64"; break }
            Default {
                Write-Error "ERROR: Unsupported architecture $($arch)"
                exit 1
            }
        }
        

        if (!$NoCache) {
            $matchingFile = Get-ChildItem $DockerfileDir | Where-Object { $_.Name -like "ravendb*$archNameToMatch*.deb" }
        }

        if(!$matchingFile) {
            $env:RAVENDB_VERSION = $version
            $env:OUTPUT_DIR = $(Convert-Path $DockerfileDir)
            $env:PACKAGE_FILE_DIR = Resolve-Path $ArtifactsDir
        
            $currentScriptWorkingDirectory = $(Get-Location)
            Set-Location $(Split-Path $buildScriptPath)
    
            Write-Host $buildScriptFileName
            . "./$buildScriptFileName"
        
            Set-Location $currentScriptWorkingDirectory
            CheckLastExitCode

            $matchingFile = Get-ChildItem $DockerfileDir | Where-Object { $_.Name -like "ravendb*$version*$archNameToMatch*.deb" }
            if ($matchingFile) {
                $pathToDeb = $matchingFile.FullName
            } else {
                Write-Host "FATAL: No ravendb .deb file for '$($arch)' architecture found after running script building .deb package." 
                exit 1
            }
        }
        else {
            $pathToDeb = $matchingFile.FullName
        }
    }
    else {
        $pathToDeb = $DebPackagePath
    }

    Write-Host "Providing deb path '$($pathToDeb)' to Dockerfile.."
    docker build $DockerfileDir -f "$($DockerfileDir)/Dockerfile.$($arch)" -t "$fullNameTag" --build-arg "PATH_TO_DEB=./$matchingFile"
    CheckLastExitCode
    
    foreach ($tag in $tags[1..$tags.Length]) {
        write-host "Tag $fullNameTag as $tag"
        docker tag "$fullNameTag" $tag
        CheckLastExitCode
    }

    Remove-Item -Path $dockerPackagePath
}

BuildUbuntuDockerImage $(GetVersionFromArtifactName) $Arch
