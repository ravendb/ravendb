function ZipFilesFromDir( $targetZipFilename, $sourcedir )
{
    $toZipGlob = [io.path]::combine($sourceDir, '*')
    Compress-Archive -Path $toZipGlob -DestinationPath $targetZipFilename
}
