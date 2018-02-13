function UnpackToDir( $archivePath, $outDir ) {
    if ($(Test-Path $outDir) -eq $False) {
        New-Item -ItemType Directory -Path $outDir
    }

    if ($archivePath.EndsWith('.tar.gz')) {
        UnpackTarGzToDir $archivePath $outDir
    }
    else {
        throw "Unsupported unpack method for $archivePath"
    }
}

function UnpackTarGzToDir ( $archivePath, $outDir ) {

    if ($($IsWindows -eq $False) -and $(Get-Command "tar" -ErrorAction SilentlyContinue)) {
        write-host "tar xvzf `"$archivePath`" -C `"$outDir`""
        & tar xvzf "$archivePath"  -C "$outDir"
        CheckLastExitCode
    }
    else {
        $7za = [io.path]::combine("scripts", "assets", "bin", "7za.exe")
        $tempTarArchiveDest = [io.path]::getDirectoryName($archivePath)
        write-host "$7za x -tgzip `"-o$tempTarArchiveDest`" $archivePath"
        & "$7za" x -tgzip "-o$tempTarArchiveDest" $archivePath
        CheckLastExitCode

        $tempTarArchivePath = $("$archivePath" -Replace "`.gz$")
        write-host "$7za x `"-o$outDir`" -ttar $tempTarArchivePath"
        & "$7za" x "-o$outDir" -ttar $tempTarArchivePath
        CheckLastExitCode

        Remove-Item $tempTarArchivePath
        CheckLastExitCode
    }
}

function CreateArchiveFromDir ( $targetFilename, $dir, $target, $wrapperDirName ) {
    if ($target.PkgType -eq "zip") {
        ZipFilesFromDir $targetFilename $dir
    }
    elseif ($target.PkgType -eq "tar.bz2") {
        TarBzFilesFromDir $targetFilename $dir $wrapperDirName
    }
    else {
        throw "Unknown archive method for $targetFilename"
    }
}

function ZipFilesFromDir( $targetFilename, $sourceDir ) {
    $toZipGlob = [io.path]::combine($sourceDir, '*')
    $zipFile = "$targetFilename.zip"
    Compress-Archive -Path "$toZipGlob" -DestinationPath "$zipFile"
}

function TarBzFilesFromDir ( $targetFilename, $sourceDir, $wrapperDirName ) {

    if ([string]::IsNullOrEmpty($wrapperDirName) -eq $false) {
        WrapContentsInDir $sourceDir $wrapperDirName
    }

    $glob = [io.path]::combine($sourceDir, '*')
    if ($($IsWindows -eq $False) -and $(Get-Command "tar" -ErrorAction SilentlyContinue)) {
        & tar -C $sourceDir -cjvf "$targetFilename.tar.bz2" .
        CheckLastExitCode
    }
    else 
    {
        $7za = [io.path]::combine("scripts", "assets", "bin", "7za.exe")
        & "$7za" a -ttar "$targetFilename.tar" $glob
        CheckLastExitCode
        & "$7za" a -tbzip2 "$targetFilename.tar.bz2" "$targetFilename.tar"
        CheckLastExitCode
        rm "$targetFilename.tar"
        CheckLastExitCode
    }
}

function WrapContentsInDir ( $packageDir, $wrapperDirName ) {
    write-host $packageDir "#" $wrapperDirName
    $wrapperDir = Join-Path $packageDir -ChildPath $wrapperDirName
    $pkgContents = Get-ChildItem -Path "$packageDir"
    New-Item -ItemType Directory -Path $wrapperDir | Out-Null
 
    Push-Location
    try {
        cd $packageDir
        foreach ($item in $pkgContents) {
            write-host "Move $item -> $wrapperDir"
            Move-Item $item $wrapperDir
        }
    } finally {
        Pop-Location
    }
}
