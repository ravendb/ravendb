function CleanDir ( $dir ) {
    write-host "Cleaning $dir..."
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $dir
    if (-Not (Test-Path -path $dir)) {
        New-Item -ItemType Directory -Path $dir | Out-Null
    }
}
function CleanBinDirs ([string[]] $srcDirs) {
    foreach ($dir in $srcDirs) {
        CleanBinDir $dir
    }
}
function CleanBinDir ( $srcDir ) {
    $binDir = [io.path]::Combine($srcDir, "bin");
    CleanDir $binDir
} 