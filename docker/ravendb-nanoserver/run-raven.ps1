$ErrorActionPreference = 'Stop'

$COMMAND=".\Raven.Server.exe"
$hostname = & "hostname.exe"
if ([string]::IsNullOrEmpty($env:RAVEN_ServerUrl) -eq $True) {
    $env:RAVEN_ServerUrl = "http://$($hostname):8080"
}

if ([string]::IsNullOrEmpty($env:RAVEN_SETTINGS) -eq $False) {
    Set-Content -Path "settings.json" -Value "$env:RAVEN_SETTINGS"
}

if ([string]::IsNullOrEmpty($env:RAVEN_ARGS) -eq $False) {
    $COMMAND = "$COMMAND $($env:RAVEN_ARGS)"
}

try {
    Invoke-Expression "$COMMAND"
} finally {
    exit $LASTEXITCODE
}

