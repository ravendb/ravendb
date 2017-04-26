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


if ([string]::IsNullOrEmpty($ConfigPath) -eq $False) {
    $fileEntry = (Get-Item $ConfigPath)

    $dockerArgs += '-v'
    $dockerArgs += "$($fileEntry.Directory):c:\raven-config"

    $dockerArgs += "-e"
    $dockerArgs += "CustomConfigFilename=$($fileEntry.Name)"

    write-host "Reading configuration from $ConfigPath"
    write-host "NOTE: due to Docker Windows containers limitations entire directory holding that file is going to be visible to the container."
}

if ([string]::IsNullOrEmpty($DataDir)) {

    if ([string]::IsNullOrEmpty($(docker volume ls | select-string $DataVolumeName))) {
        docker volume create $DataVolumeName
        write-host "Created docker volume $DataVolumeName."
    }

    $dockerArgs += "-v"
    $dockerArgs += "$($DataVolumeName):c:/databases"
} else {
    write-host "Mounting $DataDir as RavenDB data dir."
    $dockerArgs += "-v"
    $dockerArgs += "$($DataDir):c:/databases"
}

if ($AllowEverybodyToAccessTheServerAsAdmin) {
    $dockerArgs += '-e'
    $dockerArgs += "AllowEverybodyToAccessTheServerAsAdmin=true"
}


$dockerArgs += '-p'
$dockerArgs += "$($BindPort):8080"

$dockerArgs += '-p'
$dockerArgs += "$($BindTcpPort):38888"

$RAVEN_IMAGE = 'ravendb/ravendb:windows-nanoserver-latest'
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
$ravenIp = docker ps -q | % { docker inspect  -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $_ }[0];

write-host -fore white "**********************************************"
write-host ""
write-host "RavenDB docker container running."
write-host "Container ID is $containerId"
write-host ""
write-host -nonewline "To stop it use:     "
write-host -fore cyan "docker stop $containerId"
write-host -nonewline "To run shell use:   "
write-host -fore cyan "docker exec -it $containerId powershell"
write-host ""
write-host -nonewline "Access RavenDB Studio on "
write-host -fore yellow "http://$($ravenIp):$BindPort"
write-host -nonewline "Listening for TCP connections on: "
write-host -fore yellow "$($ravenIp):$BindTcpPort"
write-host ""
write-host -fore white "**********************************************"
