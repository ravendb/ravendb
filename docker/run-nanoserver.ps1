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
    $CertificatePasswordFile = "",
    $IP = "",
    $Hostname = "",
    [switch]$AuthenticationDisabled,
    [switch]$RemoveOnExit,
    [switch]$DryRun)

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

if ([string]::IsNullOrEmpty($ConfigPath) -eq $False) {
    if ($(Test-Path $ConfigPath) -eq $False) {
        throw "Config file does not exist under $ConfigPath path."
    }

    $configDir = Split-Path $ConfigPath 

    $containerConfigDir = "c:\ravendb\config"
    $containerConfigFile = Split-Path -Path $ConfigPath -Leaf
    $dockerArgs += "-v"
    $dockerArgs += "`"$($configDir):$containerConfigDir`""

    $dockerArgs += "-e"
    $envConfigPath = $containerConfigDir + '\' + $containerConfigFile 
    $dockerArgs += "`"CUSTOM_CONFIG_FILE=$envConfigPath`""

    write-host "Reading configuration from $ConfigPath"
}

if ([string]::IsNullOrEmpty($DataDir) -eq $False) {
    write-host "Mounting $DataDir as RavenDB data dir."
    $dockerArgs += "-v"
    $dockerArgs += "$($DataDir):c:/databases"
}

if ($AuthenticationDisabled) {
    $dockerArgs += '-e'
    $dockerArgs += "UNSECURED_ACCESS_ALLOWED=PublicNetwork"
}

if ([string]::IsNullOrEmpty($PublicServerUrl) -eq $False) {
    $dockerArgs += "-e" 
    $dockerArgs += "PUBLIC_SERVER_URL=$PublicServerUrl"
}

if ([string]::IsNullOrEmpty($PublicTcpServerUrl) -eq $False) {
    $dockerArgs += "-e" 
    $dockerArgs += "PUBLIC_TCP_SERVER_URL=$PublicTcpServerUrl"
}

if ([string]::IsNullOrEmpty($LogsMode) -eq $False) {
    $dockerArgs += "-e"
    $dockerArgs += "LOGS_MODE=$LogsMode"
}

if ([string]::IsNullOrEmpty($CertificatePath) -eq $False) {
    if ($(Test-Path $CertificatePath) -eq $False) {
        throw "Certificate file does not exist under $CertificatePath."
    }

    $certDir = Split-Path $CertificatePath # we have to share entire dir for windows container

    $containerCertDir = "c:\ravendb\cert"
    $containerCertFile = Split-Path -Path $CertificatePath -Leaf
    $dockerArgs += "-v"
    $dockerArgs += "`"$($certDir):$containerCertDir`""

    $dockerArgs += "-e"
    $envCertPath = $containerCertDir + '\' + $containerCertFile 
    $dockerArgs += "`"CERTIFICATE_PATH=$envCertPath`""
}

if ([string]::IsNullOrEmpty($CertificatePassword) -eq $False) {
    $dockerArgs += "-e"
    $dockerArgs += "`"CERTIFICATE_PASSWORD=$CertificatePassword`""
}

if ([string]::IsNullOrEmpty($CertificatePasswordFile) -eq $False) {
    if ($(Test-Path $CertificatePasswordFile) -eq $False) {
        throw "Certificate file does not exist under $CertificatePath."
    }

    $passDir = Split-Path $CertificatePasswordFile

    $containerPasswordDir = "C:\ravendb\secrets"
    $containerPasswordFile = Split-Path -Leaf -Path $CertificatePasswordFile

    $dockerArgs += "-v"
    $dockerArgs += "`"$($passDir):$containerPasswordDir`""

    $dockerArgs += "-e"
    $dockerArgs += "`"CERTIFICATE_PASSWORD_FILE=$($containerPasswordDir + '\' + $containerPasswordFile)`""
}

if ([string]::IsNullOrEmpty($Ip) -eq $False) {
    $dockerArgs += "--ip"
    $dockerArgs += "$IP"
}

if ([string]::IsNullOrEmpty($Hostname) -eq $False) {
    $dockerArgs += "--hostname=$Hostname"
}

$dockerArgs += '-p'
$dockerArgs += "$($BindPort):8080"

$dockerArgs += '-p'
$dockerArgs += "$($BindTcpPort):38888"

$RAVEN_IMAGE = 'ravendb/ravendb:windows-nanoserver-latest'
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

$scheme = if ([string]::IsNullOrEmpty($CertificatePath)) { "http" } else { "https" }

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
write-host -fore yellow "$($scheme)://$($ravenIp):$BindPort"
write-host -nonewline "Listening for TCP connections on: "
write-host -fore yellow "$($ravenIp):$BindTcpPort"
write-host ""
write-host -fore white "**********************************************"
