$ErrorActionPreference = 'Stop'

$COMMAND=".\Raven.Server.exe"
$hostname = & "hostname.exe"

if ([string]::IsNullOrEmpty($env:RAVEN_SETTINGS) -eq $False) {
    Set-Content -Path "settings.json" -Value "$env:RAVEN_SETTINGS"
}

function Get-RavenServerScheme {
    if (Get-Content "settings.json" -Raw | Select-String -Pattern "Server.Certificate.Path|Server.Certificate.Load.Exec") {
        return "https"
    }
    elseif (![string]::IsNullOrEmpty($env:RAVEN_Server_Certificate_Path) -or
            ![string]::IsNullOrEmpty($env:RAVEN_Server_Certificate_Load_Exec) -or
            $env:RAVEN_ARGS -like "*--Server.Certificate.Path*" -or
            $env:RAVEN_ARGS -like "*--Server.Certificate.Load.Exec*") {
        return "https"
    }
    else {
        return "http"
    }
}

if ([string]::IsNullOrEmpty($env:RAVEN_ServerUrl)) {
    $RAVEN_SERVER_SCHEME = Get-RavenServerScheme
    $env:RAVEN_ServerUrl = "$RAVEN_SERVER_SCHEME://$hostname:8080"
}

if ([string]::IsNullOrEmpty($env:RAVEN_ARGS) -eq $False) {
    $COMMAND = "$COMMAND $($env:RAVEN_ARGS)"
}

try {
    Invoke-Expression "$COMMAND"
} finally {
    exit $LASTEXITCODE
}

