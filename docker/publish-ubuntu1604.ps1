param($Version)

$ErrorActionPreference = "Stop"

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

if ([string]::IsNullOrEmpty($Version)) {
    throw "Version parameter is mandatory."
}

write-host "docker push ravendb/ravendb:$Version-ubuntu.16.04-x64"
docker push ravendb/ravendb:$Version-ubuntu.16.04-x64
CheckLastExitCode

write-host "docker push ravendb/ravendb:ubuntu-latest"
docker push ravendb/ravendb:ubuntu-latest
CheckLastExitCode

write-host "docker push ravendb/ravendb:latest"
docker push ravendb/ravendb:latest
CheckLastExitCode
