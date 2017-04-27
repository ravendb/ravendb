param(
    $BindPort = 8080,
    $BindTcpPort = 38888,
    $ConfigPath = "",
    $DataDir = "",
    $DataVolumeName = "ravendb",
    [switch]$AllowEverybodyToAccessTheServerAsAdmin,
    [switch]$RemoveOnExit)

$ErrorActionPreference = "Stop";

function CheckLastExitCode {
    param ([int[]]$SuccessCodes = @(0), [scriptblock]$CleanupScript=$null)

    if ($SuccessCodes -notcontains $LastExitCode) {
        if ($CleanupScript) {
            "Executing cleanup script: $CleanupScript"
            &$CleanupScript
        }
        $msg = @"
EXE RETURNED EXIT CODE $LastExitCode
CALLSTACK:$(Get-PSCallStack | Out-String)
"@
        throw $msg
    }
}

$dockerArgs = @('run')

# run in detached mode
$dockerArgs += '-d'

if ($RemoveOnExit) {
    $dockerArgs += '--rm'
}

if ($AllowEverybodyToAccessTheServerAsAdmin) {
    $dockerArgs += '-e'
    $dockerArgs += "AllowEverybodyToAccessTheServerAsAdmin=true"
}

if ([string]::IsNullOrEmpty($DataDir)) {

    if ([string]::IsNullOrEmpty($(docker volume ls | select-string $DataVolumeName))) {
        docker volume create $DataVolumeName
        write-host "Created docker volume $DataVolumeName."
    }

    $dockerArgs += "-v"
    $dockerArgs += "$($DataVolumeName):/databases"

} else {
    write-host "Mounting $DataDir as RavenDB data dir."
    $dockerArgs += "-v"
    $dockerArgs += "$($DataDir):/databases"
}

if ([string]::IsNullOrEmpty($ConfigPath) -eq $False) {
    $dockerArgs += "-v"
    $dockerArgs += "$($ConfigPath):/opt/raven-settings.json"
    write-host "Reading configuration from $ConfigPath"
}

$dockerArgs += '-p'
$dockerArgs += "$($BindPort):8080"

$dockerArgs += '-p'
$dockerArgs += "$($BindTcpPort):38888"

$RAVEN_IMAGE = 'ravendb/ravendb:ubuntu-latest'
$dockerArgs += $RAVEN_IMAGE

try {
    $containerId = Invoke-Expression -Command "docker $dockerArgs"
    CheckLastExitCode
} catch {
    write-host -ForegroundColor Red "Could not run docker image, please see error above for details."
    exit 1
}

write-host -nonewline "Starting container: "
write-host -fore blue "docker $dockerArgs"

start-sleep 10
$containerInSubnetAddress = docker ps -q | % { docker inspect  -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $_ }[0];

if ($IsWindows -eq $False)
{
    $ravenIp = $containerInSubnetAddress
}
else
{
    $dockerSubnetAddress = Get-NetAdapter `
        | Where-Object { $_.Name.Contains('DockerNAT') } `
        | Select-Object -Property @{
            Name = "IpV4Address";
            Expression = {
                Get-NetIPAddress -InterfaceIndex $_.ifIndex | Where-Object { $_.AddressFamily -eq "IPv4" }
            }
        }

    if (!$dockerSubnetAddress -or !$($dockerSubnetAddress.IpV4Address)) {
        throw "Could not determine Docker's subnet address."
    }

    $dockerSubnetIp = $dockerSubnetAddress.IpV4Address.ToString()

    $net = $dockerSubnetAddress.IpV4Address.ToString().Split(".") | select -first 3
    $last = $containerInSubnetAddress.ToString().Split(".") | select -skip 3
    $ravenIp = "$($net -join '.').$last"
}

$containerIdShort = $containerId.Substring(0, 10)

write-host -fore white "**********************************************"
write-host ""
write-host "RavenDB docker container running."
write-host "Container ID is $containerId"
write-host ""
write-host -nonewline "To stop it use:     "
write-host -fore cyan "docker stop $containerIdShort"
write-host -nonewline "To run shell use:   "
write-host -fore cyan "docker exec -it $containerIdShort /bin/bash"
write-host ""
write-host -nonewline "Access RavenDB Studio on "
write-host -fore yellow "http://$($ravenIp):$BindPort"
write-host -nonewline "Listening for TCP connections on: "
write-host -fore yellow "$($ravenIp):$BindTcpPort"
write-host ""

write-host -fore darkgray "Container IP address in Docker network: $containerInSubnetAddress"

if ([string]::IsNullOrEmpty($dockerSubnetIp) -eq $False) {
    write-host -fore darkgray "Docker bridge iface address: $dockerSubnetIp"
}

write-host ""

write-host -fore white "**********************************************"
