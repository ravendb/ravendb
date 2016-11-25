function CopyStudioPackage ( $studioBuildDir, $outDir ) {
    $studioZipPath = "$studioBuildDir\Raven.Studio.zip"
    write-host "Copying Studio package from $studioZipPath to $outDir\Server"
    cp $studioZipPath "$outDir\Server"
    CheckLastExitCode
}
