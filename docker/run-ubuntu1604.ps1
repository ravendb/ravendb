param(
    [switch]$Detached,
    [switch]$AllowEverybodyToAccessTheServerAsAdmin,
    [switch]$Debug,
    $BindPort = 8080,
    $BindTcpPort = 38888,
    $DbVolumeName = "ravendb",
    $ConfigPath = "")

if ([string]::IsNullOrEmpty($(docker volume ls | select-string $DbVolumeName))) {
    write-host "Create docker volume $DbVolumeName"
    docker volume create $DbVolumeName
}

$everybodyAdmin = $AllowEverybodyToAccessTheServerAsAdmin.ToString().ToLower()

if ([string]::IsNullOrEmpty($ConfigPath) -eq $False) {
    $configSwitch = "-v`"$($ConfigPath):/opt/raven-settings.json`"".Trim()
    write-host "Reading configuration from $ConfigPath."
}

if($Detached -eq $False) {
    docker run -it --rm `
        -p "$($BindPort):8080" `
        -p "$($BindTcpPort):38888" `
        -v "$($DbVolumeName):/databases" `
        $configSwitch `
        ravendb/ravendb:ubuntu-latest $debugOpt
} else {
    docker run -d `
        -p "$($BindPort):8080" `
        -p "$($BindTcpPort):38888" `
        -v "$($DbVolumeName):/databases" `
        $configSwitch `
        ravendb/ravendb:ubuntu-latest
}
