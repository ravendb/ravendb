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

$CUSTOM_SETTINGS_PATH = "c:\raven-config\$env:CustomConfigFilename"

$command = './rvn.exe'
$commandArgs = @( 'windows-service' )

$service = Get-Service | Where-Object { $_.Name -eq "RavenDB" } | Select-Object -First 1
if ($service -eq $null) {
    $commandArgs += 'register'

    $commandArgs += "--ServerUrl=http://0.0.0.0:8080"
    $commandArgs += "--ServerUrl.Tcp=tcp://0.0.0.0:38888"
    $commandArgs += "--DataDir=$($env:DataDir)"

    if ([string]::IsNullOrEmpty($env:CustomConfigFilename) -eq $False) {
        $commandArgs += "--config-path"
        $commandArgs += "`"$CUSTOM_SETTINGS_PATH`""
    }

    if ([string]::IsNullOrEmpty($env:UnsecuredAccessAllowed) -eq $False) {
        $commandArgs += "--Security.UnsecuredAccessAllowed=$($env:UnsecuredAccessAllowed)"
    }

    if ([string]::IsNullOrEmpty($env:PublicServerUrl) -eq $False) {
        $commandArgs += "--PublicServerUrl=$($env:PublicServerUrl)"
    }

    if ([string]::IsNullOrEmpty($env:PublicTcpServerUrl) -eq $False) {
        $commandArgs += "--PublicServerUrl.Tcp=$($env:PublicTcpServerUrl)"
    }

    if ([string]::IsNullOrEmpty($env:LogsMode) -eq $False) {
        $commandArgs += "--Logs.Mode=$($env:LogsMode)"
    }

    write-host "Registering Windows Service: $command $commandArgs"
} else {
    $commandArgs += "start"
    write-host "Starting existing Windows Service $command $commandArgs"
}

Invoke-Expression -Command "$command $commandArgs"
CheckLastExitCode

.\rvn.exe logstream
