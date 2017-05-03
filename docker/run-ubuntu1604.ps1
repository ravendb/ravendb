param(
    $BindPort = 8080,
    $BindTcpPort = 38888,
    $ConfigPath = "",
    $DataDir = "",
    $DataVolumeName = "ravendb",
    [switch]$AllowEverybodyToAccessTheServerAsAdmin,
    [switch]$RemoveOnExit,
    [switch]$DryRun,
    [switch]$DontScanVmSubnet)

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

function DetermineDockerVmSubnet {
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

    return $dockerSubnetAddress.IpV4Address
}

function GetDockerizedServerId ($containerId) {
    $serverIdMatch = docker logs $containerId | select-string -Pattern "Server ID is ([A-Za-z0-9-]+)."
    $serverId = $null;
    if ($serverIdMatch -and $serverIdMatch.Matches[0].Groups[1]) {
        $serverId = $serverIdMatch.Matches[0].Groups[1].Value
    }

    return $serverId
}

function DetermineServerIp ($serverId, $dockerSubnetAddress, $shouldScan) {
    $net = $dockerSubnetAddress.IpV4Address.ToString().Split(".") | Select-Object -first 3
    $netPrefix = $net -join "."

    $lastOctet = 2

    if ($serverId -and $shouldScan) {
        foreach ($scanOctet in 2..254) {
            $uri = "$netPrefix.$( $scanOctet ):$BindPort/admin/stats/server-id"
            try {
                $response = (Invoke-WebRequest -Uri $uri -TimeoutSec 1 -Method 'GET').Content | ConvertFrom-Json
                if ($response.ServerId -eq $serverId) {
                    $lastOctet = $scanOctet
                    break;
                }
            } catch {}
        }
    }

    return "$netPrefix.$lastOctet"
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
        CheckLastExitCode
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

if ($DryRun) {
    write-host -fore magenta "docker $dockerArgs"
    exit 0
}

write-host -nonewline "Starting container: "
write-host -fore magenta "docker $dockerArgs"

try {
    $containerId = Invoke-Expression -Command "docker $dockerArgs"
    CheckLastExitCode
} catch {
    write-host -ForegroundColor Red "Could not run docker image, please see error above for details."
    exit 1
}

start-sleep 5

$containerInSubnetAddress = docker ps -q -f id=$containerId | ForEach-Object { docker inspect  -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $_ }[0];

if ([string]::IsNullOrEmpty($containerInSubnetAddress)) {
    write-host -fore red "Could not determine container`'s IP address. Is it running?"
    write-host -fore red -nonewline "Try: "
    $interactiveCmdArgs = "$dockerArgs".Replace('-d', '-it --rm')
    write-host -fore magenta "docker $interactiveCmdArgs"
    exit 1
}

if ($IsWindows -eq $False)
{
    $ravenIp = $containerInSubnetAddress
}
else
{
    try {
        #
        # To avoid the need of running this script with elevated privileges,
        # we scan DockerNAT network searching for the proper server.
        # Most of the time it's the first machine on the network - x.x.x.2
        #

        $dockerSubnetAddress = DetermineDockerVmSubnet
        $serverId = GetDockerizedServerId $containerId
        $ravenIp = DetermineServerIp $serverId $dockerSubnetAddress $(-Not $DontScanVmSubnet)
    } catch {
        $ravenIp = $null
    }
}

$containerIdShort = $containerId.Substring(0, 10)

write-host -nonewline -fore white "***********************************************************"
write-host -fore red "
       _____                       _____  ____
      |  __ \                     |  __ \|  _ \
      | |__) |__ ___   _____ _ __ | |  | | |_) |
      |  _  // _` \  \ / / _ \ '_ \| |  | |  _ <
      | | \ \ (_| |\ V /  __/ | | | |__| | |_) |
      |_|  \_\__,_| \_/ \___|_| |_|_____/|____/
"
write-host -fore cyan "      Safe by default, optimized for efficiency"
write-host ""
write-host -nonewline "Container ID is "
write-host -fore white "$containerId"
write-host ""
write-host -nonewline "To stop it use:`t`t"
write-host -fore cyan "docker stop $containerIdShort"
write-host -nonewline "To run shell use:`t"
write-host -fore cyan "docker exec -it $containerIdShort /bin/bash"
write-host -nonewline "See output using:`t"
write-host -fore cyan "docker logs $containerIdShort"
write-host -nonewline "Inspect with:`t`t"
write-host -fore cyan "docker inspect $containerIdShort"

if ([string]::IsNullOrEmpty($ravenIp) -eq $False) {
    write-host ""
    write-host -nonewline "Access RavenDB Studio on "
    write-host -fore yellow "http://$($ravenIp):$BindPort"
    write-host -nonewline "Listening for TCP connections on: "
    write-host -fore yellow "$($ravenIp):$BindTcpPort"
    write-host ""
}

write-host -fore darkgray "Container IP address in Docker network: $containerInSubnetAddress"

if ($dockerSubnetAddress) {
    write-host -fore darkgray "Docker bridge iface address: $($dockerSubnetAddress.IpV4Address.ToString())"
}

write-host ""

write-host -fore white "***********************************************************"
