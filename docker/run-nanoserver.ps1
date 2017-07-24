param(
    $BindPort = 8080,
    $BindTcpPort = 38888,
    $ConfigPath = "",
    $DataDir = "",
    $PublicServerUrl = "",
    $PublicTcpServerUrl = "",
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
    $fileEntry = (Get-Item $ConfigPath)

    $dockerArgs += '-v'
    $dockerArgs += "$($fileEntry.Directory):c:\raven-config"

    $dockerArgs += "-e"
    $dockerArgs += "CustomConfigFilename=$($fileEntry.Name)"

    write-host "Reading configuration from $ConfigPath"
    write-host "NOTE: due to Docker Windows containers limitations entire directory holding that file is going to be visible to the container."
}

if ([string]::IsNullOrEmpty($DataDir) -eq $False) {
    write-host "Mounting $DataDir as RavenDB data dir."
    $dockerArgs += "-v"
    $dockerArgs += "$($DataDir):c:/databases"
}

if ($AuthenticationDisabled) {
    $dockerArgs += '-e'
    $dockerArgs += "SecurityAuthenticationEnabled=false"
}

if ([string]::IsNullOrEmpty($PublicServerUrl) -eq $False) {
    $dockerArgs += "-e" 
    $dockerArgs += "PublicServerUrl=$PublicServerUrl"
}

if ([string]::IsNullOrEmpty($PublicTcpServerUrl) -eq $False) {
    $dockerArgs += "-e" 
    $dockerArgs += "PublicTcpServerUrl=$PublicTcpServerUrl"
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
write-host -fore yellow "http://$($ravenIp):$BindPort"
write-host -nonewline "Listening for TCP connections on: "
write-host -fore yellow "$($ravenIp):$BindTcpPort"
write-host ""
write-host -fore white "**********************************************"
