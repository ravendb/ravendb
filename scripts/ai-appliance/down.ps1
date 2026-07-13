#requires -Version 5.1
<#
.SYNOPSIS
  Stop and remove the running RavenDB Quill demo container.

.PARAMETER PurgeData
  Also delete the named Docker volume (drops @quill-config and all
  per-app databases). Without this switch the volume is left in place so the
  next `up.ps1` restores state.

.PARAMETER Volume
  Docker named volume that backs /var/lib/ai-appliance. Default: ai-appliance-data.
#>
[CmdletBinding()]
param(
    [switch]$PurgeData,
    [string]$Volume = 'ai-appliance-data'
)

$ErrorActionPreference = 'Stop'

$existing = docker ps -aq --filter "name=^ai-appliance-demo$"
if ($existing) {
    Write-Host 'Stopping ai-appliance-demo...' -ForegroundColor Cyan
    docker rm -f ai-appliance-demo | Out-Null
} else {
    Write-Host 'No ai-appliance-demo container running.' -ForegroundColor DarkGray
}

if ($PurgeData) {
    Write-Host "Purging volume $Volume..." -ForegroundColor Yellow
    docker volume rm -f $Volume | Out-Null
}
