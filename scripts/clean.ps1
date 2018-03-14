function CleanDir ( $dir ) {
    write-host "Cleaning $dir..."
    if (Test-Path -path $dir) {
        Remove-Item -Recurse -Force $dir
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
}
function CleanSrcDirs ([string[]] $srcDirs) {
    foreach ($dir in $srcDirs) {
        CleanBinDir $dir
        CleanObjDir $dir
    }
}
function CleanBinDir ( $srcDir ) {
    $binDir = [io.path]::Combine($srcDir, "bin");
    CleanDir $binDir
} 

function CleanObjDir ( $srcDir ) {
    $objDir = [io.path]::Combine($srcDir, "obj");
    CleanDir $objDir
} 

function CleanFiles ( $dir ) {
    $files = Get-ChildItem -Path $RELEASE_DIR -File
    foreach ($f in $files) { 
        write-host "Remove $f..."
        Remove-Item $f.FullName 
    }
}
