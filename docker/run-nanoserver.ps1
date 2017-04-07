param(
    [switch]$Detached,
    [switch]$AllowEverybodyToAccessTheServerAsAdmin,
    $BindPort = 8080,
    $DbVolumeName = "ravendb")

if ([string]::IsNullOrEmpty($(docker volume ls | select-string $dbVolumeName))) {
    write-host "Create docker volume $dbVolumeName."
    docker volume create $dbVolumeName
}

$everybodyAdmin = $AllowEverybodyToAccessTheServerAsAdmin.ToString().ToLower()

if ($Detached -eq $False)
{
    docker run -it --rm `
        -p "$($BindPort):8080" `
        -v "$($dbVolumeName):c:/databases" `
        -e "AllowEverybodyToAccessTheServerAsAdmin=$($everybodyAdmin)" `
        ravendb/ravendb:nanoserver
}
else
{
    docker run -d `
        -p "$($BindPort):8080" `
        -v "$($dbVolumeName):c:/databases" `
        -e "AllowEverybodyToAccessTheServerAsAdmin=$($everybodyAdmin)" `
        ravendb/ravendb:nanoserver

    start-sleep -Seconds 5;
    docker ps -q | % { docker inspect  -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $_ } | % { "http://$($_):8080/" };
}
