$ErrorActionPreference='Stop'

$COMMAND=".\Raven.Server.exe"
$env:RAVEN_ServerUrl = "http://$(hostname):8080"
if ([string]::IsNullOrEmpty($env:RAVEN_ARGS) -eq $False) {
    $COMMAND = "$COMMAND $env:RAVEN_ARGS"
}

Invoke-Expression -Command "$COMMAND";
