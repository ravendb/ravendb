$RELEASE_PKG_RUNTIME_MAP = @{
    "win10-x64" = @{ "Name" = "windows-x64"; "Type" = "zip" };
    "ubuntu.14.04-x64" = @{ "Name" = "ubuntu.14.04-x64"; "Type" = "tar" };
    "ubuntu.16.04-x64" = @{ "Name" = "ubuntu.16.04-x64"; "Type" = "tar" };
}

$NETSTANDARD16 = 'netstandard1.6'

function CreateArchiveFromDir ( $targetFilename, $dir, $runtime ) {
    $spec = GetPkgSpec $runtime
    if ($spec.Type -eq "zip") {
        ZipFilesFromDir $targetFilename $dir
    } elseif ($spec.Type -eq "tar") {
        TarGzFilesFromDir $targetFilename $dir
    } else {
        throw "Unknown archive method for $targetFilename"
    }
}

function ZipFilesFromDir( $targetFilename, $sourceDir )
{
    $toZipGlob = [io.path]::combine($sourceDir, '*')
    $zipFile = "$targetFilename.zip"
    Compress-Archive -Path "$toZipGlob" -DestinationPath "$zipFile"
}

function TarGzFilesFromDir ( $targetFilename, $sourceDir ) {
    $glob = [io.path]::combine($sourceDir, '*')
    if ($(Get-Command "tar" -ErrorAction SilentlyContinue))
    {
        & tar -C $sourceDir -cvzf "$targetFilename.tar.gz" .
        CheckLastExitCode
    }
    else
    {
        $7za = [io.path]::combine("scripts", "assets", "bin", "7za.exe")
        & "$7za" a -ttar "$targetFilename.tar" $glob
        CheckLastExitCode
        & "$7za" a -tgzip "$targetFilename.tar.gz" "$targetFilename.tar"
        CheckLastExitCode
        rm "$targetFilename.tar"
    }
}

function CreateRavenPackage ( $projectDir, $releaseDir, $outDir, $version, $runtime ) {
    write-host "Create package for $runtime..."

    $releaseArchiveFile = GetRavenArchiveFileName $version $runtime
    $releaseArchivePath = [io.path]::combine($releaseDir, $releaseArchiveFile)
    $packageDir = [io.path]::combine($outDir, "package")
    New-Item -ItemType Directory -Path $packageDir

    CreatePackageLayout $outDir $packageDir $projectDir
    CreateArchiveFromDir $releaseArchivePath $packageDir $runtime
}

function GetRavenArchiveFileName ( $version, $runtime ) {
    $pkgSpec = GetPkgSpec $runtime
    "RavenDB-$version-$($pkgSpec.Name)"
}

function GetPkgSpec ($runtime) {
    if ($RELEASE_PKG_RUNTIME_MAP.ContainsKey($runtime) -eq $False) {
        throw "Do not have pkg spec for $runtime."
    }

    $RELEASE_PKG_RUNTIME_MAP.Get_Item($runtime)
}

function CreatePackageLayout ( $outDir, $packageDir, $projectDir ) {
    CopyLicenseFile $packageDir
    CopyAckFile $packageDir
    CreatePackageServerLayout $outDir $packageDir
    CreatePackageClientLayout $outDir $packageDir $projectDir
    CopyClientReadMe $(Join-Path $packageDir -ChildPath 'Client')
}

function CreatePackageServerLayout ( $outDir, $packageDir ) {
    write-host "Create package server directory layout..."

    $serverOutDir = [io.path]::combine($outDir, "Server")
    cp -r $serverOutDir $packageDir
}

function CreatePackageClientLayout ( $outDir, $packageDir, $projectDir ) {
    write-host "Create package client directory layout..."

    $clientOutDir = [io.path]::combine($outDir, "Client")

    $ravenClientDir = [io.path]::combine($packageDir, 'Client', $NETSTANDARD16, 'Raven.Client')
    $ravenClientDllDir = [io.path]::combine($ravenClientDir, $NETSTANDARD16)
    New-Item -ItemType Directory -Path $ravenClientDllDir

    $sparrowDir = [io.path]::combine($packageDir, 'Client', $NETSTANDARD16, 'Sparrow')
    $sparrowDllDir = [io.path]::combine($packageDir, 'Client', $NETSTANDARD16, 'Sparrow', $NETSTANDARD16)
    New-Item -ItemType Directory -Path $sparrowDllDir

    cp $(Join-Path $clientOutDir -ChildPath "Raven.Client.dll") $ravenClientDllDir
    cp $(Join-Path $clientOutDir -ChildPath "Raven.Client.pdb") $ravenClientDllDir
    cp $(Join-Path $clientOutDir -ChildPath "Sparrow.dll") $sparrowDllDir
    cp $(Join-Path $clientOutDir -ChildPath "Sparrow.pdb") $sparrowDllDir

    $assetsDir = [io.path]::combine($projectDir, "scripts", "assets", "pkg")
    $ravenClientAssetsDir = [io.path]::combine($assetsDir, "Raven.Client")
    $sparrowAssetsDir = [io.path]::combine($assetsDir, "Sparrow")

    $ravenClientProjectTemplate = Get-Content -Raw -Path $(Join-Path $ravenClientAssetsDir "project.json.template") | ConvertFrom-Json
    $ravenClientProjectOrig = Get-Content -Raw -Path $([io.path]::combine($projectDir, "src", "Raven.Client",  "project.json")) | ConvertFrom-Json
    $ravenClientProjectTemplate.dependencies = $ravenClientProjectOrig.dependencies
    $ravenClientProjectTemplate `
        | ConvertTo-Json -Depth 100 `
        | Out-File $(Join-Path $ravenClientDir -ChildPath "project.json") -Encoding UTF8

    cp $(Join-Path $ravenClientAssetsDir -ChildPath "Raven.Client.xproj.template") $(Join-Path $ravenClientDir -ChildPath  "Raven.Client.xproj")
    cp $(Join-Path $sparrowAssetsDir -ChildPath "Sparrow.xproj.template") $(Join-Path $sparrowDir -ChildPath "Sparrow.xproj")
    cp $(Join-Path $sparrowAssetsDir -ChildPath "project.json.template") $(Join-Path $sparrowDir -ChildPath "project.json")
}
