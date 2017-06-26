$DOTNET_URL = "https://ravendb-raspberry-pi.s3.amazonaws.com/dotnet-ubuntu.14.04-arm.1.2.0-beta-001291-00.tar.gz"
$DOTNET_ARCHIVE_NAME = "dotnet.tar.gz"
function DownloadDotnetRuntimeForUbuntu14Arm32 ( $outDir, $cacheDir ) {
    $targetPath = [io.path]::combine($outDir, $DOTNET_ARCHIVE_NAME);

    if (-Not $(Test-Path $cacheDir)) {
        New-Item -ItemType Directory $cacheDir 
    }

    $cachedDotnetPath = [io.path]::combine($cacheDir, $DOTNET_ARCHIVE_NAME)

    if (Test-Path -Path $cachedDotnetPath) {
        Copy-Item -Path $cachedDotnetPath -Destination $targetPath
        return
    }

    Invoke-WebRequest -Uri $DOTNET_URL -OutFile $cachedDotnetPath -TimeoutSec 900
    Copy-Item -Path $cachedDotnetPath -Destination $targetPath
}
