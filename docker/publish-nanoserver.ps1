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

write-host "docker push ravendb/ravendb:$Version-windows-nanoserver"
docker push ravendb/ravendb:$Version-windows-nanoserver
CheckLastExitCode

write-host "docker push ravendb/ravendb:windows-nanoserver-latest"
docker push ravendb/ravendb:windows-nanoserver-latest
CheckLastExitCode