#requires -Version 5.1
<#
.SYNOPSIS
  Stop and remove the running RavenDB Quill demo container.

.PARAMETER PurgeData
  Also delete the named Docker volume (drops @quill-config and all
  per-app databases). Without this switch the volume is left in place so the
  next `up.ps1` restores state.

.PARAMETER Volume
  Docker named volume that backs /var/lib/quill. Default: quill-data.
#>
[CmdletBinding()]
param(
    [switch]$PurgeData,
    [string]$Volume = 'quill-data'
)

$ErrorActionPreference = 'Stop'

$existing = docker ps -aq --filter "name=^quill-demo$"
if ($existing) {
    Write-Host 'Stopping quill-demo...' -ForegroundColor Cyan
    docker rm -f quill-demo | Out-Null
} else {
    Write-Host 'No quill-demo container running.' -ForegroundColor DarkGray
}

if ($PurgeData) {
    Write-Host "Purging volume $Volume..." -ForegroundColor Yellow
    docker volume rm -f $Volume | Out-Null
}
