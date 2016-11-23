function CleanBuildDirectories($releaseDir, $outDir, $buildDir, $temp) {
    CleanDir $releaseDir
    CleanDir $outDir
    CleanDir $buildDir
}

function CleanDir ( $dir ) {
    write-host "Cleaning $dir..."
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $dir
    if (-Not (Test-Path -path $dir)) {
        mkdir $dir
    }
}
