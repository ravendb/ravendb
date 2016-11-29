$RELEASE_ZIP_RUNTIME_MAP = @{
    "win10-x64" = "windows-x64"
}

$NETSTANDARD16 = 'netstandard1.6'

function ZipFilesFromDir( $targetZipFilename, $sourcedir )
{
    $toZipGlob = [io.path]::combine($sourceDir, '*')
    Compress-Archive -Path $toZipGlob -DestinationPath $targetZipFilename
}

function CreateRavenPackage ( $projectDir, $releaseDir, $outDir, $version, $runtime ) {
    write-host "Create ZIP package..."

    $releaseZipFile = GetRavenZipFileName $version $runtime
    $releaseZipPath = [io.path]::combine($releaseDir, $releaseZipFile)
    $packageDir = [io.path]::combine($outDir, "package")
    New-Item -ItemType Directory -Path $packageDir

    CreatePackageLayout $outDir $packageDir $projectDir
    ZipFilesFromDir $releaseZipPath $packageDir
}

function GetRavenZipFileName ( $version, $runtime ) {
    if ($RELEASE_ZIP_RUNTIME_MAP.ContainsKey($runtime)) {
        $runtimeText = $RELEASE_ZIP_RUNTIME_MAP.Get_Item($runtime)
    } else {
        $runtimeText = $runtime
    }

    "RavenDB-$version-$runtimeText.zip"
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
