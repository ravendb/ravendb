function ZipFilesFromDir( $targetZipFilename, $sourcedir )
{
    write-host "Creating ZIP file $targetZipFilename"
    Add-Type -Assembly System.IO.Compression.FileSystem
    $compressionLevel = [System.IO.Compression.CompressionLevel]::Optimal
    [System.IO.Compression.ZipFile]::CreateFromDirectory($sourcedir, $targetZipFilename, $compressionLevel, $false)
}
