param(
    $Repo = "ravendb/ravendb",
    $DockerSettingsFile = "src\Raven.Server\Properties\Settings\settings.docker.windows.json")

$ErrorActionPreference = "Stop"
function BuildWindowsDockerImage ( $projectDir, $version, $repo, $settingsFile) {
    $packageFileName = "RavenDB-$version-windows-x64.zip"
    $packagePath = [io.path]::combine($projectDir, "artifacts", $packageFileName)

    if ([string]::IsNullOrEmpty($packagePath))
    {
        throw "PackagePath cannot be empty."
    }

    if ($(Test-Path $packagePath) -eq $False) {
        throw "Package file does not exist."
    }

    Copy-Item -Path $packagePath -Destination ./ravendb-nanoserver/RavenDB.zip -Force

    $settingsPath = Join-Path -Path $projectDir -ChildPath $settingsFile
    Copy-Item -Path $settingsPath -Destination ./ravendb-nanoserver/settings.json -Force

    write-host "Build docker image: $version"
    write-host "Tags: $($repo):$version-windows-nanoserver $($repo):windows-nanoserver-latest"

    docker build ./ravendb-nanoserver `
        -t "$($repo):$version-windows-nanoserver" `
        -t "$($repo):windows-nanoserver-latest"

    Remove-Item "./ravendb-nanoserver/RavenDB.zip"
}

function GetVersionFromArtifactName() {
    $versionRegex = [regex]'RavenDB-([0-9]\.[0-9]\.[0-9](-[a-zA-Z]+-[0-9-]+)?)-[a-z]+'
    $fname = $(Get-ChildItem "../artifacts" `
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

BuildWindowsDockerImage ".." $(GetVersionFromArtifactName) $Repo $DockerSettingsFile
