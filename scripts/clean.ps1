function CleanDir ( $dir ) {
    write-host "Cleaning $dir..."
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $dir
    if (-Not (Test-Path -path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
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

function CleanFiles ( $dir ) {
    $files = Get-ChildItem -Path $RELEASE_DIR -File
    foreach ($f in $files) { 
        write-host "Remove $f..."
        Remove-Item $f.FullName 
    }
}
