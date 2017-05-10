$ErrorActionPreference = "Stop";

$versionPath = ".\version.txt";
$executablePath = ".\Server\Raven.Server.exe";
$assemblyVersion =  & $executablePath --version;
$version = $null;

if (Test-Path $versionPath) {
    $version = Get-Content -Path $versionPath;
}

if ($version -ne $assemblyVersion) {
    Set-Content -Path $versionPath $assemblyVersion;
    Start-Process "http://ravendb.net/first-run?type=start&ver=$assemblyVersion";
}

Start-Process "$executablePath" -ArgumentList ('--browser');
