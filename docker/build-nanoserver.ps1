param(
    $Repo = "ravendb/ravendb",
    $ArtifactsDir = "..\artifacts",
    $RavenDockerSettingsPath = "..\src\Raven.Server\Properties\Settings\settings.docker.windows.json",
    $DockerfileDir = "./ravendb-nanoserver")

$ErrorActionPreference = "Stop"
function BuildWindowsDockerImage ($version) {
    $packageFileName = "RavenDB-$version-windows-x64.zip"
    $artifactsPackagePath = Join-Path -Path $ArtifactsDir -ChildPath $packageFileName

    if ([string]::IsNullOrEmpty($artifactsPackagePath))
    {
        throw "PackagePath cannot be empty."
    }

    if ($(Test-Path $artifactsPackagePath) -eq $False) {
        throw "Package file does not exist."
    }
    
    $dockerPackagePath = Join-Path -Path $DockerfileDir -ChildPath "RavenDB.zip"
    Copy-Item -Path $artifactsPackagePath -Destination $dockerPackagePath -Force
    Copy-Item -Path $RavenDockerSettingsPath -Destination $(Join-Path -Path $DockerfileDir -ChildPath "settings.json") -Force


    write-host "Build docker image: $version"
    write-host "Tags: $($repo):$version-windows-nanoserver $($repo):4.2-windows-nanoserver-latest"

    docker build $DockerfileDir `
        -t "$($repo):windows-nanoserver-latest" `
        -t "$($repo):$version-windows-nanoserver" `
        -t "$($repo):4.2-windows-nanoserver-latest"

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

    if (!$Version) {
        throw "Could not parse version from artifact file name: $fname"
    }

    return $version
}

BuildWindowsDockerImage $(GetVersionFromArtifactName)
