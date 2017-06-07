$CUSTOM_SETTINGS_PATH = "c:\raven-config\$env:CustomConfigFilename"

cd c:/ravendb/Server

$command = './Raven.Server.exe'
$commandArgs = @()

$commandArgs += "/Raven/ServerUrl=http://0.0.0.0:8080"
$commandArgs += "/Raven/ServerUrl/Tcp=tcp://0.0.0.0:38888"
$commandArgs += "/Raven/DataDir=$($env:DataDir)"
$commandArgs += "--print-id"
$commandArgs += "--register-service"

if ([string]::IsNullOrEmpty($env:CustomConfigFilename) -eq $False) {
    $commandArgs += "--config-path `"$CUSTOM_SETTINGS_PATH`""
}

if ([string]::IsNullOrEmpty($env:AllowAnonymousUserToAccessTheServer) -eq $False) {
    $commandArgs += "/Raven/AllowAnonymousUserToAccessTheServer=$($env:AllowAnonymousUserToAccessTheServer)"
}

if ([string]::IsNullOrEmpty($env:PublicServerUrl)) {
    $commandArgs += "/Raven/PublicServerUrl=$($env:PublicServerUrl)"
}

if ([string]::IsNullOrEmpty($env:PublicTcpServerUrl)) {
    $commandArgs += "/Raven/PublicServerUrl/Tcp=$($env:PublicTcpServerUrl)"
}

Invoke-Expression -Command "$command $commandArgs"

while ($true) { 
    Start-Sleep 60 
    $serviceStatus = (Get-Service -Name "RavenDB").Status
    if (($serviceStatus -eq "Running") -or ($serviceStatus -eq "StartPending")) {
        continue;
    } else {
        break;
    }
}
