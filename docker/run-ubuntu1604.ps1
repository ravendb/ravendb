param(
    [switch]$Detached,
    [switch]$Debug,
    $BindPort = 8080,
    $BindTcpPort = 38888,
    $DataDir = "",
    $ConfigPath = "",
    $DataVolumeName = "ravendb")

if ([string]::IsNullOrEmpty($DataDir)) {

    if ([string]::IsNullOrEmpty($(docker volume ls | select-string $DataVolumeName))) {
        docker volume create $DataVolumeName
        write-host "Created docker volume $DataVolumeName."
    }

    $dataVolumeMountOpt = $DataVolumeName
} else {
    write-host "Mounting $DataDir as RavenDB data dir."
    $dataVolumeMountOpt = $DataDir
}

if ([string]::IsNullOrEmpty($ConfigPath) -eq $False) {
    $configSwitch = "-v`"$($ConfigPath):/opt/raven-settings.json`"".Trim()
    write-host "Reading configuration from $ConfigPath."
}

if ($Debug) {
    $debugOpt = "bash"
}

if($Detached -eq $False) {
    docker run -it --rm `
        -p "$($BindPort):8080" `
        -p "$($BindTcpPort):38888" `
        -v "$($dataVolumeMountOpt):/databases" `
        $configSwitch `
        ravendb/ravendb:ubuntu-latest $debugOpt
} else {
    docker run -d `
        -p "$($BindPort):8080" `
        -p "$($BindTcpPort):38888" `
        -v "$($dataVolumeMountOpt):/databases" `
        $configSwitch `
        ravendb/ravendb:ubuntu-latest
}
