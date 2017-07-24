param(
    $ServerDir="c:/ravendb/Server")
    
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

Push-Location $ServerDir

$command = './rvn.exe'
$commandArgs = @( 'windows-service', 'register' )

$commandArgs += "--Security.Authentication.RequiredForPublicNetworks=false"
$commandArgs += "--ServerUrl=http://0.0.0.0:8080"
$commandArgs += "--ServerUrl.Tcp=tcp://0.0.0.0:38888"
$commandArgs += "--DataDir=$($env:DataDir)"

if ([string]::IsNullOrEmpty($env:CustomConfigFilename) -eq $False) {
    $commandArgs += "--config-path"
    $commandArgs += "`"$CUSTOM_SETTINGS_PATH`""
}

if ([string]::IsNullOrEmpty($env:SecurityAuthenticationEnabled) -eq $False) {
    $commandArgs += "--Security.Authentication.Enabled=$($env:SecurityAuthenticationEnabled)"
}

if ([string]::IsNullOrEmpty($env:PublicServerUrl) -eq $False) {
    $commandArgs += "--PublicServerUrl=$($env:PublicServerUrl)"
}

if ([string]::IsNullOrEmpty($env:PublicTcpServerUrl) -eq $False) {
    $commandArgs += "--PublicServerUrl.Tcp=$($env:PublicTcpServerUrl)"
}

write-host "Registering Windows Service: $command $commandArgs"
Invoke-Expression -Command "$command $commandArgs"
CheckLastExitCode

while ($true) { 
    Start-Sleep 60 
    $serviceStatus = (Get-Service -Name "RavenDB").Status
    if (($serviceStatus -eq "Running") -or ($serviceStatus -eq "StartPending")) {
        continue;
    } else {
        break;
    }
}
