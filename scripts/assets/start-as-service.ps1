function Get-ScriptDirectory
{
    $Invocation = (Get-Variable MyInvocation -Scope 1).Value;
    Split-Path $Invocation.MyCommand.Path;
}

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
	Write-Host "Error. Port must be in the range 0-65535."
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
    exit 3
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
    exit 1
}
Finally
{
    Pop-Location;
}

Write-Host "Service started, server listening to http://127.0.0.1:$portval"

start "http://127.0.0.1:$portval"

