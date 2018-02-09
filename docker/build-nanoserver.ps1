$ErrorActionPreference = "Stop"

function BuildWindowsDockerImage ( $projectDir, $version) {
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

    $settingsPath = Join-Path -Path $projectDir -ChildPath "src\Raven.Server\Properties\Settings\settings.docker.windows.json"
    Copy-Item -Path $settingsPath -Destination ./ravendb-nanoserver/settings.json -Force

    docker build ./ravendb-nanoserver `
        -t ravendb/ravendb:$version-windows-nanoserver `
        -t ravendb/ravendb:windows-nanoserver-latest

    Remove-Item "./ravendb-nanoserver/RavenDB.zip"
}

$versionRegex = [regex]'RavenDB-([0-9]\.[0-9]\.[0-9](-[a-zA-Z]+-[0-9-]+)?)-[a-z]+'
$fname = $(Get-ChildItem "../artifacts" | Where-Object { $_.Name -Match $versionRegex } | Select-Object -First 1).Name
$match = $fname | select-string -Pattern $versionRegex
$version = $match.Matches[0].Groups[1]

BuildWindowsDockerImage ".." $Version
