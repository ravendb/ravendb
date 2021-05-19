
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
                "$($repo):latest",
                "$($repo):latest-lts",
                "$($repo):ubuntu-latest",
                "$($repo):ubuntu-latest-lts",
                "$($repo):5.2-ubuntu-latest",
                "$($repo):$($version)-ubuntu.20.04-x64"
            )
            break;
        }
        "arm32v7" {
            return @(
                "$($repo):ubuntu-arm32v7-latest",
                "$($repo):ubuntu-arm32v7-latest-lts",
                "$($repo):5.2-ubuntu-arm32v7-latest",
                "$($repo):$($version)-ubuntu.20.04-arm32v7"
            )
            break;
        }
        Default {
            throw "Arch not supported."
        }
    }
        
}
