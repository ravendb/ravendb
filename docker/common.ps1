
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
