#Requires -Version 4.0

function Get-ScriptDirectory
{
    $Invocation = (Get-Variable MyInvocation -Scope 1).Value;
    Split-Path $Invocation.MyCommand.Path;
}

$ErrorActionPreference = "Stop";

$scriptDirectory = Get-ScriptDirectory;
$versionPath = Join-Path $scriptDirectory "version.txt";
$executable = "Raven.Server.exe";
$executableDir = Join-Path $scriptDirectory "Server"
$executablePath = Join-Path $executableDir $executable;
$assemblyVersion =  & $executablePath --version;
$version = $null;

if (Test-Path $versionPath) {
    $version = Get-Content -Path $versionPath;
}

if ($version -ne $assemblyVersion) {
    Set-Content -Path $versionPath $assemblyVersion;
    Start-Process "http://ravendb.net/first-run?type=start&ver=$assemblyVersion";
}

$args = @( "--browser" );

Push-Location $executableDir;

Try
{
    Invoke-Expression -Command ".\$executable $args";
    if ($LASTEXITCODE -ne 0) { 
        Read-Host -Prompt "Press enter to continue...";
    }
}
Finally
{
    Pop-Location;
}
