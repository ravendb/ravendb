function DownloadDotnetForRPi ( $outDir ) {
    $url = "https://ravendb-raspberry-pi.s3.amazonaws.com/dotnet.tar.bz2"
    $target = [io.path]::combine($outDir, "dotnet.tar.bz2");
    Invoke-WebRequest -Uri $url -OutFile $target -TimeoutSec 900
}