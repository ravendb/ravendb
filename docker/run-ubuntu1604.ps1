param(
    [switch]$Detached,
    [switch]$AllowEverybodyToAccessTheServerAsAdmin,
    $BindPort = 8080,
    $DbVolumeName = "ravendb")

if ([string]::IsNullOrEmpty($(docker volume ls | select-string $dbVolumeName))) {
    write-host "Create docker volume $dbVolumeName"
    docker volume create $dbVolumeName
}

$everybodyAdmin = $AllowEverybodyToAccessTheServerAsAdmin.ToString().ToLower()

if($Detached -eq $False) {
    docker run `
        --rm `
        -p "$($BindPort):8080" `
        -it `
        -v "$($dbVolumeName):/databases" `
        -e "AllowEverybodyToAccessTheServerAsAdmin=$($everybodyAdmin)" `
        ravendb/ravendb:ubuntu-latest
} else {
    docker run `
        -d `
        -p "$($BindPort):8080" `
        -v "$($dbVolumeName):/databases" `
        -e "AllowEverybodyToAccessTheServerAsAdmin=$everybodyAdmin" `
        ravendb/ravendb:ubuntu-latest
}
