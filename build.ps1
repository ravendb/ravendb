param($task = "default")

$scriptPath = $MyInvocation.MyCommand.Path
$scriptDir = Split-Path $scriptPath

get-module psake | remove-module

.nuget\NuGet.exe restore .nuget\packages.config -OutputDirectory packages
.nuget\NuGet.exe restore zzz_RavenDB_Release.sln -OutputDirectory packages
import-module (Get-ChildItem "$scriptDir\packages\psake.*\tools\psake.psm1" | Select-Object -First 1)

exec { invoke-psake "$scriptDir\default.ps1" $task }