param($task = "default")

$scriptPath = $MyInvocation.MyCommand.Path
$scriptDir = Split-Path $scriptPath

get-module psake | remove-module

.nuget\NuGet.exe install .nuget\packages.config -OutputDirectory packages
import-module (Get-ChildItem "$scriptDir\packages\psake.*\tools\psake.psm1" | Select-Object -First 1)

invoke-psake "$scriptDir\default.ps1" $task