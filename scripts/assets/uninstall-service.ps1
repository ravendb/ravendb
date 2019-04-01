#Requires -Version 4.0
#Requires -RunAsAdministrator
$ErrorActionPreference = "Stop";
$ProgressPreference = "SilentlyContinue";

function Get-ScriptDirectory
{
    $Invocation = (Get-Variable MyInvocation -Scope 1).Value;
    Split-Path $Invocation.MyCommand.Path;
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


$scriptDirectory = Get-ScriptDirectory;
$rvn = "rvn.exe";
$serverDir = Join-Path $scriptDirectory "Server"

$name = 'RavenDB'
 
Push-Location $serverDir;

Try
{
    $existingService = Get-Service -Name $name # just checking if it's installed

    $serviceStatus = $existingService.Status
    write-host "Service '$name' status is: $serviceStatus."
    if (($serviceStatus -eq "Running") -or ($serviceStatus -eq "StartPending")) {
        write-host "Stopping '$name' service..."
        Stop-Service -Name $name
    }

    Invoke-Expression -Command ".\$rvn windows-service unregister --service-name $name";
    CheckLastExitCode
}
catch
{
    write-error $_.Exception
    exit 1
}
finally
{
    Pop-Location;
}

