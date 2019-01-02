param([switch]$DryRun)
    
$ErrorActionPreference = "Stop"

$ARTIFACTS = Get-ChildItem $([io.path]::combine("artifacts", '*')) -Include "*.zip", "*.tar", "*.tar.bz2"

. '.\scripts\version.ps1'
. '.\scripts\checkLastExitCode.ps1'
. '.\scripts\upload.ps1'
. '.\scripts\getScriptDirectory.ps1'

$projectDir = Get-ScriptDirectory
$uploader = $env:UPLOADER_PATH
$versionInfo = GetVersionInfo
$files = Get-ChildItem $ARTIFACTS

$filesString = $files -join "`r`n"
write-host "Found artifacts: `r`n$filesString"

Upload "$uploader" $versionInfo $files $DryRun
