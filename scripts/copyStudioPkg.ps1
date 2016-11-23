function CopyStudioPackage ( $studioBuildDir, $outDir ) {
    $studioZipPath = "$studioBuildDir\Raven.Studio.zip"
    write-host "Copying Studio package from $studioZipPath to $OUT_DIR\Server"
    cp $studioZipPath "$OUT_DIR\Server"
    CheckLastExitCode
}
