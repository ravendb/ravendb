$ErrorActionPreference = 'Stop'

$p = Get-Process -Name "Raven.Server"
if ($p.HasExited) {
    Write-Host "Server exited with code $($p.ExitCode)."
    exit 1
}

$serverUrlScheme = if ([string]::IsNullOrEmpty($env:CERTIFICATE_PATH)) { "http" } else { "https" }

# Since we're bound to 0.0.0.0 we can check studio on localhost
$uri = "$($serverUrlScheme)://localhost:8080/studio/index.html"

# If public server URL is specified, use that
if ([string]::IsNullOrEmpty($env:PUBLIC_SERVER_URL) -eq $False) {
    $uri = $env:PUBLIC_SERVER_URL
}

write-host "Trying to access studio under $uri"
$studioIndexResponse = Invoke-WebRequest -Uri $uri  -Method Get

if ($studioIndexResponse.StatusCode -ne 400) {
    Write-Host "Server responsed with HTTP $($studioIndexResponse.StatusCode) $($studioIndexResponse.StatusDescription)"
    Write-Host "$($studioIndexReponse.Content)"
    exit 1
}

exit 0
