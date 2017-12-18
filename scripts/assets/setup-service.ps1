function Get-ScriptDirectory
{
    $Invocation = (Get-Variable MyInvocation -Scope 1).Value;
    Split-Path $Invocation.MyCommand.Path;
}

# this assumes that we are setting up the service and starting the wizard

$ErrorActionPreference = "Stop";

$scriptDirectory = Get-ScriptDirectory;
$settingsJson = "settings.json";
$rvn = "rvn.exe";
$serverDir = Join-Path $scriptDirectory "Server"
$settingsJsonPath = Join-Path $serverDir $settingsJson;

$name = Read-Host -Prompt 'Please enter the service name'
$port = Read-Host -Prompt 'Please enter a port number'
[int]$portval = [int]::Parse($port)

if ($portval -lt 0 -Or $portval -gt 65535){
	Write-Error "Error. Port must be in the range 0-65535."
	exit 1
}

try
{
	$json = Get-Content $settingsJsonPath -raw | ConvertFrom-Json
	$json.ServerUrl="http://127.0.0.1:$portval"
	$json | ConvertTo-Json  | set-content $settingsJsonPath
}
catch
{
    write-error $_.Exception
    exit 2
}

Push-Location $serverDir;

Try
{
    Invoke-Expression -Command ".\$rvn windows-service register --service-name $name";
    Start-Service -Name $name
}
catch
{
    write-error $_.Exception
    exit 3
}
Finally
{
    Pop-Location;
}

Write-Host "Service started, server listening to http://127.0.0.1:$portval"
Write-Host "You can now finish setting up the RavenDB service in the browser"

sleep 3 # ugly, but need to give it time to startup

start "http://127.0.0.1:$portval"

