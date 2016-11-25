function CopyStudioPackage ( $studioBuildDir, $outDir ) {
    $studioZipPath = [io.path]::combine($studioBuildDir, "Raven.Studio.zip")
    write-host "Copying Studio package from $studioZipPath to $outDir\Server"
    $serverOutDir = [io.path]::combine($outDir, "Server")
    cp $studioZipPath $serverOutDir
    CheckLastExitCode
}
