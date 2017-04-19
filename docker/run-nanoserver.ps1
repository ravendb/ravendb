param(
    [switch]$Detached,
    [switch]$Debug,
    $BindPort = 8080,
    $BindTcpPort = 38888,
    $DbVolumeName = "ravendb",
    $ConfigPath = "")

if ([string]::IsNullOrEmpty($(docker volume ls | select-string $dbVolumeName))) {
    write-host "Create docker volume $dbVolumeName."
    docker volume create $dbVolumeName
}

if ([string]::IsNullOrEmpty($ConfigPath) -eq $False) {
    $fileEntry = (get-item $ConfigPath)
    $configSwitch = "-v`"$($fileEntry.Directory):c:\raven-config`"".Trim()
    $configFilenameSwitch = "-e`"CustomConfigFilename=$($fileEntry.Name)`""
    write-host "Reading configuration from $ConfigPath"
    write-host "NOTE: due to Docker Windows containers limitations entire directory holding that file is going to be visible to the container."
}

if ($Debug) {
    $debugOpt = "powershell"
}

if ($Detached -eq $False)
{
    docker run -it --rm `
        -p "$($BindPort):8080" `
        -p "$($BindTcpPort):38888" `
        -v "$($dbVolumeName):c:/databases" `
        $configSwitch `
        $configFilenameSwitch `
        ravendb/ravendb:windows-nanoserver-latest $debugOpt
}
else
{
    docker run -d `
        -p "$($BindPort):8080" `
        -p "$($BindTcpPort):38888" `
        -v "$($dbVolumeName):c:/databases" `
        $configSwitch `
        $configFilenameSwitch `
        ravendb/ravendb:windows-nanoserver-latest

    start-sleep -Seconds 5;
    docker ps -q | % { docker inspect  -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $_ } | % { "http://$($_):8080/" };
}
