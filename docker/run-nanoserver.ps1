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
    [switch]$Unsecured,
    [switch]$NoSetup = $False,
    [switch]$RemoveOnExit,
    [switch]$DryRun,
    [switch]$UseNightly = $False)

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
$ravenArgs = @('--log-to-console')

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
    $dockerArgs += "$($DataDir):C:/RavenDB/Server/RavenData"
}

if ([string]::IsNullOrEmpty($ConfigPath) -eq $False) {

    if ($(Test-Path $ConfigPath) -eq $False) {
        throw "Config file does not exist under $ConfigPath path."
    }

    $configDir = Split-Path $ConfigPath 

    $containerConfigDir = "C:\RavenDB\Config"
    $containerConfigFile = Split-Path -Path $ConfigPath -Leaf
    $dockerArgs += "-v"
    $dockerArgs += "`"$($configDir):$containerConfigDir`""

    $envConfigPath = $containerConfigDir + '\' + $containerConfigFile 
    $ravenArgs += "-c '$envConfigPath'"

    write-host "Reading configuration from $ConfigPath"
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

if ([string]::IsNullOrEmpty($NoSetup) -eq $False) {
    $ravenArgs += "--Setup.Mode=None"
}

$serverUrlScheme = 'http'

if ([string]::IsNullOrEmpty($CertificatePath) -eq $False) {
    if ($(Test-Path $CertificatePath) -eq $False) {
        throw "Certificate file does not exist under $CertificatePath."
    }

    $certDir = Split-Path $CertificatePath # we have to share entire dir for windows container

    $containerCertDir = "C:\RavenDB\cert"
    $containerCertFile = Split-Path -Path $CertificatePath -Leaf
    $dockerArgs += "-v"
    $dockerArgs += "`"$($certDir):$containerCertDir`""

    $dockerArgs += "-e"
    $envCertPath = $containerCertDir + '\' + $containerCertFile 
    $dockerArgs += "`"RAVEN_Security_Certificate_Path=$envCertPath`""

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
    $RAVEN_IMAGE = 'ravendb/ravendb-nightly:4.2-windows-nanoserver-latest'
} else {
    $RAVEN_IMAGE = 'ravendb/ravendb:4.2-windows-nanoserver-latest'
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

start-sleep 10
$ravenIp = docker ps -q -f id=$containerId | % { docker inspect  -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $_ }[0];

if ([string]::IsNullOrEmpty($ravenIp)) {
    write-host -fore red "Could not determine container`'s IP address. Is it running?"
    write-host -fore red -nonewline "Try: "
    $interactiveCmdArgs = "$dockerArgs".Replace('-d', '-it --rm')
    write-host -fore magenta "docker $interactiveCmdArgs"
    exit 1
}

$containerIdShort = $containerId.Substring(0, 10)

write-host -nonewline -fore white "**********************************************"
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
write-host -fore cyan "docker exec -it $containerIdShort powershell"
write-host -nonewline "See output using:`t"
write-host -fore cyan "docker logs $containerIdShort"
write-host -nonewline "Inspect with:`t`t"
write-host -fore cyan "docker inspect $containerIdShort"
write-host ""
write-host -nonewline "Access RavenDB Studio on "
write-host -fore yellow "$($serverUrlScheme)://$($ravenIp):$BindPort"
write-host -nonewline "Listening for TCP connections on: "
write-host -fore yellow "$($ravenIp):$BindTcpPort"
write-host ""
write-host -fore white "**********************************************"
