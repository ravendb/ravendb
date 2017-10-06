$ErrorActionPreference = 'Stop'

$p = Get-Process -Name "Raven.Server"
if ($p.HasExited) {
    Write-Host "Server exited with code $($p.ExitCode)."
    exit 1
}

exit 0
