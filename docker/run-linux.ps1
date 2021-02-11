param(
    $BindPort = 8080,
    $BindTcpPort = 38888,
    $ConfigPath = "",
    $DataDir = "",
    $PublicServerUrl = "",
    $PublicTcpServerUrl = "",
    $LogsMode = "",
    $CertificatePath = "",
    $CertificatePassword = "",
    $Hostname = "",
    [switch]$RemoveOnExit,
    [switch]$DryRun,
    [switch]$DontScanVmSubnet,
    [switch]$Unsecured = $False,
    [switch]$UseNightly = $False,
    [switch]$NoSetup = $False,
    [string]$Memory)

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
        write-host -NoNewLine "Searching for server..."
        foreach ($scanOctet in 2..254) {
            write-host -NoNewLine "."

            $uri = "$netPrefix.$( $scanOctet ):$BindPort/debug/server-id"
            try {
                $response = (Invoke-WebRequest -Uri $uri -TimeoutSec 1 -Method 'GET' -UseBasicParsing).Content | ConvertFrom-Json
                if ($response.ServerId -eq $serverId) {
                    $lastOctet = $scanOctet
                    break;
                }
            } catch {}
        }
    }

    write-host ""

    return "$netPrefix.$lastOctet"
}

$dockerArgs = @('run')
$ravenArgs = @('--print-id', '--log-to-console')

# run in detached mode
$dockerArgs += '-d'

if ($RemoveOnExit) {
    $dockerArgs += '--rm'
}

if ($Unsecured) {
    $dockerArgs += '-e'
    $dockerArgs += "RAVEN_Security_UnsecuredAccessAllowed=PublicNetwork"
}

if ([string]::IsNullOrEmpty($DataDir) -eq $False) {
    write-host "Mounting $DataDir as RavenDB data dir."
    $dockerArgs += "-v"
    $dockerArgs += "`"$($DataDir):/opt/RavenDB/Server/RavenData`""
}

if ([string]::IsNullOrEmpty($ConfigPath) -eq $False) {
    if ($(Test-Path $ConfigPath) -eq $False) {
        throw "Config file does not exist under $ConfigPath path."
    }

    $configDir = Split-Path $ConfigPath 

    $containerConfigDir = "/opt/RavenDB/config"
    $containerConfigFile = Split-Path -Path $ConfigPath -Leaf
    $dockerArgs += "-v"
    $dockerArgs += "`"$($configDir):$containerconfigDir`""

    $envConfigPath = $containerConfigDir + '/' + $containerConfigFile 
    $ravenArgs += "-c '$envConfigPath'"

    write-host "Reading configuration from $ConfigPath"
}

if ([string]::IsNullOrEmpty($Memory) -eq $False) {
    $dockerArgs += "--memory=" + $Memory
    write-host "Memory limited to " + $memory
}

if ([string]::IsNullOrEmpty($PublicServerUrl) -eq $False) {
    $dockerArgs += "-e" 
    $dockerArgs += "RAVEN_PublicServerUrl=$PublicServerUrl"
}

if ([string]::IsNullOrEmpty($PublicTcpServerUrl) -eq $False) {
    $dockerArgs += "-e" 
    $dockerArgs += "RAVEN_PublicServerUrl_Tcp=$PublicTcpServerUrl"
}

if ([string]::IsNullOrEmpty($LogsMode) -eq $False) {
    $dockerArgs += "-e"
    $dockerArgs += "RAVEN_Logs_Mode=$LogsMode"
}

if ($NoSetup) {
    $ravenArgs += "--Setup.Mode=None"
}

$serverUrlScheme = 'http'

if ([string]::IsNullOrEmpty($CertificatePath) -eq $False) {
    if ($(Test-Path $CertificatePath) -eq $False) {
        throw "Certificate file does not exist under $CertificatePath."
    }

    $containerCertDir = "/opt/RavenDB/cert"
    $containerCertFile = Split-Path -Leaf -Path $CertificatePath

    $hostDir = Split-Path $CertificatePath

    $dockerArgs += "-v"
    $dockerArgs += "`"$($hostDir):$containerCertDir`""

    $dockerArgs += "-e"
    $dockerArgs += "`"RAVEN_Security_Certificate_Path=$($containerCertDir + '/' + $containerCertFile)`""

    $serverUrlScheme = 'https'
    $ravenArgs += "--ServerUrl=https://0.0.0.0:8080"
}

if ([string]::IsNullOrEmpty($CertificatePassword) -eq $False) {
    $dockerArgs += "-e"
    $dockerArgs += "`"RAVEN_Security_Certificate_Password=$CertificatePassword`""
}

if ([string]::IsNullOrEmpty($Ip) -eq $False) {
    $dockerArgs += "--ip"
    $dockerArgs += "$IP"
}

if ([string]::IsNullOrEmpty($Hostname) -eq $False) {
    $dockerArgs += "--hostname=$Hostname"
}

if ([string]::IsNullOrEmpty($ravenArgs) -eq $False) {
    $dockerArgs += "-e"
    $dockerArgs += "RAVEN_ARGS='$ravenArgs'"
}

$dockerArgs += '-p'
$dockerArgs += "$($BindPort):8080"

$dockerArgs += '-p'
$dockerArgs += "$($BindTcpPort):38888"

if ($UseNightly) {
    $RAVEN_IMAGE = 'ravendb/ravendb-nightly:5.2-ubuntu-latest'
} else {
    $RAVEN_IMAGE = 'ravendb/ravendb:5.2-ubuntu-latest'
}

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
    write-host -fore yellow "$($serverUrlScheme)://$($ravenIp):$BindPort"
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
