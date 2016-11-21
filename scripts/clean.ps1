function CleanBuildDirectories($releaseDir, $outDir, $buildDir) {
    Remove-Item -Recurse -Force $releaseDir
    Remove-Item -Recurse -Force $outDir
    Remove-Item -Recurse -Force $buildDir
}
