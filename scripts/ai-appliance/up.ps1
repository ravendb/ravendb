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
  Publish RavenDB's secure port 443 so you can open
  https://a.egor-ai.ravendb.run/ in a browser and reach Studio. Off by default
  (the design's loopback-only acceptance §3.9 stays intact for the normal demo
  run). Also auto-imports the admin client cert from the demo zip into
  Cert:\CurrentUser\My so Chrome can authenticate; idempotent (re-runs skip
  the import if the thumbprint is already in the store).

  *.ravendb.run has public-DNS A records pointing to 127.0.0.1, so no
  hosts-file edit is needed on the Windows side. Chrome will prompt to pick
  the imported client cert on the first request to Studio.

.PARAMETER StudioHostPort
  Host port to publish for RavenDB's 443. Default: 443. Use a non-privileged
  port (e.g. 8443) if 443 is taken by another service; the browser URL then
  becomes https://a.egor-ai.ravendb.run:<port>/ and the cert still validates
  (it's bound to the domain, not the port). Only meaningful with -WithStudio.

.PARAMETER ApiKey
  Operator API key the dashboard login validates against (QUILL_API_KEY). Demo
  default: 'egor'. The dashboard /login screen and the api.* surface authenticate
  with this; it is required (auth fails closed when unset).

.PARAMETER LicenseKey
  Activation token (QUILL_LICENSE_KEY) the appliance uses to pull its setup
  package at startup. Demo default: 'egor'. Ignored in demo/mock mode (the mounted
  setup-package zip answers any token), but set for parity with production.

.NOTES
  Demo setup-package zip: this script mounts $env:APPLIANCE_E2E_SETUP_PACKAGE_PATH
  (the same env you use for the AiApplianceTests E2E suite) at the Dockerfile-
  pinned in-container path /var/lib/ai-appliance/setup-source.zip. Startup
  activation (ApplianceActivationService) serves it via the mock license client in
  demo mode instead of calling the real license API. Production builds won't set
  this env and won't ship a zip; the appliance dials the real license API instead.
#>
[CmdletBinding()]
param(
    [switch]$Rebuild,
    [string]$Tag = 'ravendb/ai-appliance:demo',
    [int]$Port = 5000,
    [string]$Volume = 'ai-appliance-data',
    [switch]$WithStudio,
    [int]$StudioHostPort = 443,
    [string]$ApiKey = 'egor',
    [string]$LicenseKey = 'egor'
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
    '-v', "${Volume}:/var/lib/ai-appliance",
    # Operator auth: QUILL_API_KEY gates the dashboard login + the api.* surface
    # (required; auth fails closed without it). QUILL_LICENSE_KEY is the activation
    # token (ignored in demo/mock mode, where the mounted zip answers any token).
    '-e', "QUILL_API_KEY=$ApiKey",
    '-e', "QUILL_LICENSE_KEY=$LicenseKey"
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
    Write-Host "-WithStudio enabled: publishing RavenDB on https://localhost:$StudioHostPort and importing admin cert." -ForegroundColor Yellow
    $runArgs += @('-p', "${StudioHostPort}:443")

    # Auto-import the admin client cert from the demo zip so Chrome can
    # authenticate to Studio. RavenDB rejects any client cert that isn't in
    # its well-known-admin list AND that the local cert store also trusts; we
    # set up the server side via RAVEN_Security_WellKnownCertificates_Admin
    # (the s6 run script + activation endpoint handle that), and the browser
    # side here.
    if ($demoZip -and (Test-Path $demoZip)) {
        # Extract admin.client.certificate.*.pfx from the demo zip into a
        # temp dir, then import to the current user's Personal store. The
        # PFX is unprotected (RavenDB setup wizard generates them without a
        # password); Import-PfxCertificate still requires a SecureString, so
        # we pass an empty one.
        $tmp = Join-Path ([System.IO.Path]::GetTempPath()) ([Guid]::NewGuid().ToString('N'))
        New-Item -ItemType Directory -Path $tmp | Out-Null
        try {
            Expand-Archive -Path $demoZip -DestinationPath $tmp -Force
            $pfx = Get-ChildItem -Path $tmp -Filter 'admin.client.certificate.*.pfx' | Select-Object -First 1
            if ($null -ne $pfx) {
                $emptyPwd = New-Object System.Security.SecureString
                $loaded   = New-Object System.Security.Cryptography.X509Certificates.X509Certificate2 ($pfx.FullName, $emptyPwd)
                $existingCert = Get-ChildItem -Path Cert:\CurrentUser\My |
                    Where-Object { $_.Thumbprint -eq $loaded.Thumbprint } |
                    Select-Object -First 1
                if ($null -eq $existingCert) {
                    Import-PfxCertificate -FilePath $pfx.FullName `
                        -CertStoreLocation Cert:\CurrentUser\My `
                        -Password $emptyPwd | Out-Null
                    Write-Host "  Imported admin cert (thumbprint $($loaded.Thumbprint)) to Cert:\CurrentUser\My" -ForegroundColor DarkGray
                } else {
                    Write-Host "  Admin cert already in Cert:\CurrentUser\My (thumbprint $($loaded.Thumbprint))" -ForegroundColor DarkGray
                }
            } else {
                Write-Warning "-WithStudio: no admin.client.certificate.*.pfx found inside $demoZip; Studio will return 403 until you import the cert manually."
            }
        } finally {
            Remove-Item -Path $tmp -Recurse -Force -ErrorAction SilentlyContinue
        }
    } else {
        Write-Warning "-WithStudio without `$env:APPLIANCE_E2E_SETUP_PACKAGE_PATH set: nothing to import. Studio will return 403 until you import the admin cert manually."
    }
}
$runArgs += $Tag

Write-Host "Starting $Tag on http://localhost:$Port (volume: $Volume)..." -ForegroundColor Cyan
& docker @runArgs | Out-Null

Write-Host ''
Write-Host "Dashboard: http://localhost:$Port - sign in on /login with the API key '$ApiKey'." -ForegroundColor Green
Write-Host "Activation runs automatically at startup; wait for status Ready at /api/bootstrap/status (~30-60s)." -ForegroundColor DarkGray

if ($WithStudio) {
    # RavenDB's root path 302-redirects to /studio/index.html; the bare URL is
    # what the user types and the browser follows the redirect transparently.
    $studioUrl = if ($StudioHostPort -eq 443) { 'https://a.egor-ai.ravendb.run/' } else { "https://a.egor-ai.ravendb.run:$StudioHostPort/" }
    Write-Host ''
    Write-Host "Studio: $studioUrl  (Chrome will prompt for client cert on first request)" -ForegroundColor Green
}

Write-Host ''
Write-Host 'Tailing logs (Ctrl+C to detach; container keeps running):' -ForegroundColor Cyan
docker logs -f ai-appliance-demo
