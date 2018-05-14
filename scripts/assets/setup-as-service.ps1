#Requires -Version 4.0
#Requires -RunAsAdministrator
$ErrorActionPreference = "Stop";
$ProgressPreference = "SilentlyContinue";

function Get-ScriptDirectory
{
    $Invocation = (Get-Variable MyInvocation -Scope 1).Value;
    Split-Path $Invocation.MyCommand.Path;
}

function CheckPortIsClosed($port) {
    $result = Test-NetConnection -Port $port -ComputerName 127.0.0.1 -InformationLevel Quiet 3> $null
    return $result -eq $false
}

function SetAclOnServerDirectory($dir) {
    $acl = Get-Acl $dir
    $permissions = "LocalService", "FullControl", "ContainerInherit, ObjectInherit", "None", "Allow"
    $rule = New-Object -TypeName System.Security.AccessControl.FileSystemAccessRule -ArgumentList $permissions
    $acl.SetAccessRuleProtection($False, $False)
    $acl.AddAccessRule($rule)
    Set-Acl -Path $dir -AclObject $acl
}

$scriptDirectory = Get-ScriptDirectory;
$settingsTemplateJson = "settings.default.json";
$settingsJson = "settings.json";
$rvn = "rvn.exe";
$serverDir = Join-Path $scriptDirectory "Server"

SetAclOnServerDirectory $(Join-Path -Path $scriptDirectory -ChildPath "Server")

$settingsJsonPath = Join-Path $serverDir $settingsJson
$settingsTemplateJsonPath = Join-Path $serverDir $settingsTemplateJson;

$name = 'RavenDB'
 
$isAlreadyConfigured = Test-Path $settingsJsonPath

if ($isAlreadyConfigured) {
    write-host "Server was run before - attempt to use existing configuration."
    $serverUrl = $(Get-Content $settingsJsonPath -raw | ConvertFrom-Json).ServerUrl
} else {
    write-host "Server run for the first time."
    $secure = Read-Host -Prompt 'Would you like to setup a secure server? (y/n)'

    if ($secure -match '^\s*?[yY]') {
        $port = 443
    }
    else {
        $port = 8080
    }

    if ($port -lt 0 -Or $port -gt 65535) {
        Write-Error "Error. Port must be in the range 0-65535."
        exit 1
    }

    if ((CheckPortIsClosed $port) -eq $false) {
        Write-Error "Port $port is not available.";
        exit 2
    }

    try {
        $json = Get-Content $settingsTemplateJsonPath -raw | ConvertFrom-Json
        $serverUrl = $json.ServerUrl = "http://127.0.0.1:$port"
        $json | ConvertTo-Json  | Set-Content $settingsTemplateJsonPath
    }
    catch {
        Write-Error $_.Exception
        exit 3
    }
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
    exit 4
}
finally
{
    Pop-Location;
}

Write-Host "Service started, server listening on $serverUrl."
Write-Host "You can now finish setting up the RavenDB service in the browser."

Start-Sleep -Seconds 3
Start-Process $serverUrl 
