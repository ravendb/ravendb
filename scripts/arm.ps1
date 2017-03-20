$DOTNET_URL = "https://ravendb-raspberry-pi.s3.amazonaws.com/dotnet-ubuntu.14.04-arm.1.2.0-beta-001291-00.tar.gz"
function DownloadDotnetRuntimeForUbuntu14Arm32 ( $outDir ) {
    $dlPath = [io.path]::combine($outDir, "dotnet.tar.gz");
    Invoke-WebRequest -Uri $DOTNET_URL -OutFile $dlPath -TimeoutSec 900
}
