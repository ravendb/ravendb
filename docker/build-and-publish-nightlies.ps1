param([switch]$DryRun = $False, [switch]$RemoveImages = $True)

$ErrorActionPreference = 'Stop'

$UBUNTU = 'ubuntu'
$WINDOWS = 'windows'
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

function GetDaemonMode() {
    $result = (docker info | select-string -Pattern '^OSType:\s(.*)').Matches[0].Groups[1];
    CheckLastExitCode
    return $result.Value.Trim()
}
function SwitchDaemon () {
    & 'C:\Program Files\Docker\Docker\DockerCLI.exe' -SwitchDaemon
    CheckLastExitCode
}

function Get-ImageTagSuffix($platform) {
    if ($platform -eq "$WINDOWS") {
        return "windows-nanoserver"
    } 
    
    if ($platform -eq "$UBUNTU") {
        return "ubuntu.16.04-x64"
    }

    throw "Platform not supported."
}
function Get-ScriptDirectory
{
  $Invocation = (Get-Variable MyInvocation -Scope 1).Value
  Split-Path $Invocation.MyCommand.Path
}

function GetImageTags($version, $platform) {
    if ($platform -eq "$UBUNTU") {
        return @(
            "ravendb/ravendb-nightly:latest",
            "ravendb/ravendb-nightly:ubuntu-latest",
            "ravendb/ravendb-nightly:$($version)-ubuntu.16.04-x64"
        )
    } 

    if ($platform -eq "$WINDOWS") {
        return @(
            "ravendb/ravendb-nightly:windows-nanoserver-latest",
            "ravendb/ravendb-nightly:$($version)-windows-nanoserver"
        )
    }

    throw "Platform not supported."
}

function RemoveImages($tags) {
    foreach ($tag in $tags) {
        docker image rm $tag
    }
}
function GetBuildCommand() {
    param ($DockerDir, $Platform, $ImageTags)
    
    $result = @('docker', 'build');

    foreach ($tag in $imageTags) {
        $result += "-t" 
        $result += $tag
    }

    $result += $dockerDir 

    return "$result";
}

function DetermineVersionFromPackageFilename($packageWildcard) {
    $packageFile = Get-Item -Path $packageWildcard | Select-Object -First 1
    $match = [regex]::Match($packageFile.Name, "^RavenDB-(.*)-(windows|linux)-x64.(zip|tar.bz2)$")
    $version = $match.Groups[1].Value
    return $version
}

function PushImagesToDockerHub($imageTags) {
    write-host "Pushing images to Docker Hub."
    foreach ($tag in $imageTags) {
        docker push "$tag"
        CheckLastExitCode
    }
}

function PushImagesDryRun($imageTags) {
    write-host "DRY RUN: Pushing images."
    foreach ($tag in $imageTags) {
        write-host "DRY RUN: docker push $tag"
    }
}

function PushImages($imageTags) {
    if ($DryRun -eq $False) {
        PushImagesToDockerHub $imageTags
    } else {
        PushImagesDryRun $imageTags
    }
}

$daemonMode = GetDaemonMode
write-host "Docker daemon mode: $daemonMode"
if ($daemonMode -ne "linux") {
    write-host "Switch Docker daemon to Linux"
    SwitchDaemon
}

Push-Location .

try {
    $mainDockerDir = Get-ScriptDirectory
    Set-Location $mainDockerDir # cd $project/docker

    #build ubuntu image
    $linuxPackageWildcard = "..\artifacts\*nightly-*-*-linux-x64.tar.bz2"
    $version = DetermineVersionFromPackageFilename $linuxPackageWildcard
    Copy-Item -Path $linuxPackageWildcard -Destination ".\ravendb-ubuntu1604\RavenDB.tar.bz2" -Force

    $settingsPath = "..\src\Raven.Server\Properties\Settings\settings.docker.posix.json"
    Copy-Item -Path $settingsPath -Destination ./ravendb-ubuntu1604/settings.json -Force

    $ubuntuImageTags = GetImageTags $version $UBUNTU
    $buildCmd = GetBuildCommand -DockerDir ".\ravendb-ubuntu1604" -Platform $UBUNTU -ImageTags $ubuntuImageTags

    write-host "Building Ubuntu docker image: $buildCmd"
    Invoke-Expression "$buildCmd" 
    CheckLastExitCode

    PushImages $ubuntuImageTags
    if ($RemoveImages -eq $True) {
        RemoveImages $ubuntuImageTags
    }

    # switch daemon to windows engine
    SwitchDaemon

    # build windows image
    $winPackageWildcard = "..\artifacts\RavenDB-4.0.0-nightly-*-*-windows-x64.zip"
    $version = DetermineVersionFromPackageFilename $winPackageWildcard
    Copy-Item -Path $winPackageWildcard -Destination ".\ravendb-nanoserver\RavenDB.zip" -Force

    $settingsPath = "..\src\Raven.Server\Properties\Settings\settings.docker.windows.json"
    Copy-Item -Path $settingsPath -Destination ./ravendb-nanoserver/settings.json -Force
    
    $windowsImageTags = GetImageTags $version $WINDOWS
    $buildCmd = GetBuildCommand -DockerDir ".\ravendb-nanoserver" -Platform $WINDOWS -ImageTags $windowsImageTags

    write-host "Building Windows docker image: $buildCmd"
    Invoke-Expression "$buildCmd" 
    CheckLastExitCode

    PushImages $windowsImageTags

    if ($RemoveImages -eq $True) {
        RemoveImages $windowsImageTags
    }

} finally {
    Pop-Location
}




