#requires -Version 5.1
<#
.SYNOPSIS
  Build and run the RavenDB AI Appliance demo image.

.DESCRIPTION
  Builds the single-image appliance from the RavenDB repo root and runs the
  container with the data volume mounted under a named Docker volume.

.PARAMETER Rebuild
  Force `docker build --no-cache`. Otherwise normal layer caching applies.

.PARAMETER Tag
  Image tag. Default: ravendb/ai-appliance:demo.

.PARAMETER Port
  Host port to publish for the web app. Default: 5000.

.PARAMETER Volume
  Docker named volume that backs /var/lib/ai-appliance. Default:
  ai-appliance-data. A named volume (vs a host bind-mount) sidesteps the 9p
  filesystem on Docker Desktop / Windows, which doesn't expose the file-lock
  semantics RavenDB needs.

.PARAMETER WithStudio
  Publish RavenDB's port 8080 so you can open http://localhost:8080/studio in
  a browser. Off by default; the design's loopback-only acceptance (§3.9)
  stays intact for the normal demo run.

  Note: Docker Desktop on Windows doesn't reliably forward 127.0.0.1-bound
  publishes, so this switch publishes 0.0.0.0:8080. Studio will be reachable
  from any host on your LAN while the demo runs.

.NOTES
  Demo setup-package zip: this script mounts $env:APPLIANCE_E2E_SETUP_PACKAGE_PATH
  (the same env you use for the AiApplianceTests E2E suite) at the Dockerfile-
  pinned in-container path /var/lib/ai-appliance/setup-source.zip. The activation
  endpoint reads from there in demo mode instead of calling the license API.
  Production builds won't set this env and won't ship a zip; the appliance falls
  back to the HTTP path automatically.
#>
[CmdletBinding()]
param(
    [switch]$Rebuild,
    [string]$Tag = 'ravendb/ai-appliance:demo',
    [int]$Port = 5000,
    [string]$Volume = 'ai-appliance-data',
    [switch]$WithStudio
)

$ErrorActionPreference = 'Stop'

# scripts/ai-appliance/ → ../../ → ravendb repo root.
$repoRoot = (Resolve-Path "$PSScriptRoot\..\..").Path

Write-Host "Building $Tag from $repoRoot..." -ForegroundColor Cyan

Push-Location $repoRoot
try {
    $buildArgs = @(
        'build',
        '-f', 'docker/ai-appliance/Dockerfile',
        '-t', $Tag
    )
    if ($Rebuild) { $buildArgs += '--no-cache' }
    $buildArgs += '.'

    & docker @buildArgs
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
finally {
    Pop-Location
}

$existing = docker ps -aq --filter "name=^ai-appliance-demo$"
if ($existing) {
    Write-Host 'Removing previous ai-appliance-demo container...' -ForegroundColor DarkGray
    docker rm -f ai-appliance-demo | Out-Null
}

$runArgs = @(
    'run', '-d',
    '--name', 'ai-appliance-demo',
    # Activation triggers a graceful StopApplication() from inside the .NET
    # host; --restart=unless-stopped makes Docker bring the container back so
    # the next start sees the freshly-extracted setup package and connects
    # securely.
    '--restart=unless-stopped',
    '-p', "${Port}:5000",
    '-v', "${Volume}:/var/lib/ai-appliance"
)
# Mount the demo setup-package zip if APPLIANCE_E2E_SETUP_PACKAGE_PATH points
# at one. The container path is hardcoded in the Dockerfile via
# RAVEN_AI_SETUP_PACKAGE_ZIP — the appliance reads from there in demo mode.
$demoZip = $env:APPLIANCE_E2E_SETUP_PACKAGE_PATH
if ($demoZip -and (Test-Path $demoZip)) {
    $resolvedZip = (Resolve-Path $demoZip).Path
    Write-Host "Mounting demo setup-package zip: $resolvedZip" -ForegroundColor DarkGray
    $runArgs += @('-v', "${resolvedZip}:/var/lib/ai-appliance/setup-source.zip:ro")
}
if ($WithStudio) {
    Write-Host '-WithStudio enabled: publishing RavenDB on http://localhost:8080 (LAN-reachable on Windows; see help).' -ForegroundColor Yellow
    $runArgs += @('-p', '8080:8080')
}
$runArgs += $Tag

Write-Host "Starting $Tag on http://localhost:$Port (volume: $Volume)..." -ForegroundColor Cyan
& docker @runArgs | Out-Null

Write-Host ''
Write-Host 'Tailing logs (Ctrl+C to detach; container keeps running):' -ForegroundColor Cyan
docker logs -f ai-appliance-demo
