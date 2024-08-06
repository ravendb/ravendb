
function GetVersionFromArtifactName() {
    $versionRegex = [regex]'RavenDB-([0-9]\.[0-9]\.[0-9]+(-[a-zA-Z]+-[0-9-]+)?)-[a-z]+'
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

function CheckLastExitCode {
    param ([int[]]$SuccessCodes = @(0), [scriptblock]$CleanupScript=$null)

    if ($SuccessCodes -notcontains $LastExitCode) {
        if ($CleanupScript) {
            "Executing cleanup script: $CleanupScript"
            &$CleanupScript
        }
        $msg = @"
EXE RETURNED EXIT CODE $LastExitCode
CALLSTACK:$(Get-PSCallStack | Out-String)
"@
        throw $msg
    }
}

function GetUbuntuImageTags($repo, $version, $arch) {
    switch ($arch) {
        "x64" { 
            return @(
#                "$($repo):ubuntu-latest",
                "$($repo):6.2-ubuntu-latest",
                "$($repo):$($version)-ubuntu.22.04-x64"
            )
            break;
        }
        "arm32v7" {
            return @(
#                "$($repo):ubuntu-arm32v7-latest",
                "$($repo):6.2-ubuntu-arm32v7-latest",
                "$($repo):$($version)-ubuntu.22.04-arm32v7"
            )
            break;
        }
        "arm64v8" {
            return @(
#                "$($repo):ubuntu-arm64v8-latest",
                "$($repo):6.2-ubuntu-arm64v8-latest",
                "$($repo):$($version)-ubuntu.22.04-arm64v8"
                )
                break;
        }
        Default {
            throw "Arch not supported."
        }
    }
        
}

function GetWindowsImageTags($repo, $version, $WinVer) {
    switch ($winver) {
        "1809" {
            return @(
                "$($repo):$($version)-windows-1809",
#                "$($repo):windows-1809-latest",
                "$($repo):6.2-windows-1809-latest"
            )
            break;
        }
        "ltsc2022" {
             return @(
                "$($repo):$($version)-windows-ltsc2022",
#                "$($repo):windows-ltsc2022-latest",
                "$($repo):6.2-windows-ltsc2022-latest"
            )
            break;
        }
        Default{
            throw "Windows Version not supported. There are 'ltsc2022' and '1809' avaliable."
        }        
    }

}

function GetManifestTags {
    param (
        $repo
    )

    return @(
#        "${repo}:latest",
        "${repo}:6.2-latest"
    )
}

function GetImageTagsForManifest {
    param (
        [string]$repo
    )

    return @(
        "${repo}:6.2-ubuntu-latest",
        "${repo}:6.2-ubuntu-arm32v7-latest",
        "${repo}:6.2-ubuntu-arm64v8-latest",
        "${repo}:6.2-windows-1809-latest",
        "${repo}:6.2-windows-ltsc2022-latest"
    )
}
