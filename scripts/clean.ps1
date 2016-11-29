function CleanBuildDirectories($releaseDir) {
    CleanDir $releaseDir
}

function CleanDir ( $dir ) {
    write-host "Cleaning $dir..."
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $dir
    if (-Not (Test-Path -path $dir)) {
        New-Item -ItemType Directory -Path $dir
    }
}
