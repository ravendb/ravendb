$NETSTANDARD16 = 'netstandard1.6'

function CreateArchiveFromDir ( $targetFilename, $dir, $spec ) {
    if ($spec.PkgType -eq "zip") {
        ZipFilesFromDir $targetFilename $dir
    } elseif ($spec.PkgType -eq "tar") {
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
    }
}

function CreateRavenPackage ( $projectDir, $releaseDir, $outDirs, $spec, $version ) {
    write-host "Create package for $($spec.runtime)..."
    $packageDir = [io.path]::combine($outDirs.Main, "package")
    New-Item -ItemType Directory -Path $packageDir

    CreatePackageLayout $packageDir $projectDir $outDirs $spec

    $releaseArchiveFile = GetRavenArchiveFileName $version $spec
    $releaseArchivePath = [io.path]::combine($releaseDir, $releaseArchiveFile)
    CreateArchiveFromDir $releaseArchivePath $packageDir $spec
}

function GetRavenArchiveFileName ( $version, $spec ) {
    "RavenDB-$version-$($spec.Name)"
}

function CreatePackageLayout ( $packageDir, $projectDir, $outDirs, $spec ) {
    CopyLicenseFile $packageDir
    CopyAckFile $packageDir
    CreatePackageServerLayout $($outDirs.Server) $packageDir $projectDir $spec
    CreatePackageClientLayout $outDirs $packageDir $projectDir
    CopyClientReadMe $(Join-Path $packageDir -ChildPath 'Client')

    if ($spec.IsUnix) {
        CopyDaemonScripts $projectDir $packageDir
        CopyLinuxScripts $projectDir $packageDir
    }
}

function CopyLinuxScripts ( $projectDir, $packageDir ) {
    write-host "Copy Linux scripts..."

    $scriptsDir = [io.path]::combine($projectDir, "scripts", "raspberry-pi")
    $scriptsList = @( "start.sh", "setup.sh")

    Foreach ($scriptName in $scriptsList) {
        $scriptPath = Join-Path $scriptsDir -ChildPath $scriptName

        cp $scriptPath $packageDir
        CheckLastExitCode

        if ($(Get-Command "chmod" -ErrorAction SilentlyContinue)) {
            $scriptInPkgPath = Join-Path $packageDir -ChildPath $scriptName
            & "chmod" a+x $scriptInPkgPath
            CheckLastExitCode
        }
    }
}

function CopyDaemonScripts ( $projectDir, $packageDir ) {
    write-host "Copy daemon files..."

    $scriptsDir = [io.path]::combine($projectDir, "scripts", "raspberry-pi")
    $scriptsList = @( "ravendbd", "ravendb.watchdog.sh" )

    Foreach ($scriptName in $scriptsList) {
        $scriptPath = Join-Path $scriptsDir -ChildPath $scriptName
        cp $scriptPath $packageDir
        CheckLastExitCode
    }
}

function CreatePackageServerLayout ( $serverOutDir, $packageDir, $projectDir ) {
    write-host "Create package server directory layout..."

    if ($spec.Name -eq "raspberry-pi") {
        del $([io.path]::combine($serverOutDir, "*.so"))
        del $([io.path]::combine($serverOutDir, "*.ni.*"))
    }

    cp -r $serverOutDir $packageDir
}

function CreatePackageClientLayout ( $outDirs, $packageDir, $projectDir, $spec ) {
    if ($spec.Name -eq "raspberry-pi") {
        CreateRaspberryPiClientLayout $outDirs $packageDir $projectDir
    } else {
        CreateRegularPackageClientLayout $outDirs $packageDir $projectDir
    }
}

function CreateRaspberryPiClientLayout ( $outDirs, $packageDir, $projectDir ) {
    $clientOutDir = $outDirs.Client
    $clientPkgDir = [io.path]::combine($packageDir, "Client")
    New-Item -ItemType Directory -Path $clientPkgDir

    cp $(Join-Path $clientOutDir -ChildPath "Raven.Client.dll") $clientPkgDir
    cp $(Join-Path $clientOutDir -ChildPath "Raven.Client.pdb") $clientPkgDir
    cp $(Join-Path $clientOutDir -ChildPath "Sparrow.dll") $clientPkgDir
    cp $(Join-Path $clientOutDir -ChildPath "Sparrow.pdb") $clientPkgDir

    $newClientOutDir = $outDirs.NewClient
    cp $(Join-Path $newClientOutDir -ChildPath "Raven.NewClient.dll") $clientPkgDir
    cp $(Join-Path $newClientOutDir -ChildPath "Raven.NewClient.pdb") $clientPkgDir
}

function CreateRegularPackageClientLayout( $outDirs, $packageDir, $projectDir ) {
    write-host "Create package client directory layout..."

    $assetsDir = [io.path]::combine($projectDir, "scripts", "assets", "pkg")

    CopyClient $outDirs.Client $packageDir $assetsDir
    CopySparrow $outDirs.Client $packageDir $assetsDir

    $newClientOutDir = [io.path]::combine($outDir, "NewClient")
    CopyNewClient $outDirs.NewClient $packageDir $assetsDir
}

function CopyClient ( $clientOutDir, $packageDir, $assetsDir) {
    # layout client dir structure
    $ravenClientAssetsDir = [io.path]::combine($assetsDir, "Raven.Client")
    $ravenClientDir = [io.path]::combine($packageDir, 'Client', $NETSTANDARD16, 'Raven.Client')
    $ravenClientDllDir = [io.path]::combine($ravenClientDir, $NETSTANDARD16)
    New-Item -ItemType Directory -Path $ravenClientDllDir

    cp $(Join-Path $clientOutDir -ChildPath "Raven.Client.dll") $ravenClientDllDir
    cp $(Join-Path $clientOutDir -ChildPath "Raven.Client.pdb") $ravenClientDllDir

    $ravenClientProjectTemplate = Get-Content -Raw -Path $(Join-Path $ravenClientAssetsDir "project.json.template") | ConvertFrom-Json
    $ravenClientProjectOrig = Get-Content -Raw -Path $([io.path]::combine($projectDir, "src", "Raven.Client",  "project.json")) | ConvertFrom-Json
    $ravenClientProjectTemplate.dependencies = $ravenClientProjectOrig.dependencies
    $ravenClientProjectTemplate `
    | ConvertTo-Json -Depth 100 `
    | Out-File $(Join-Path $ravenClientDir -ChildPath "project.json") -Encoding UTF8

    cp $([io.path]::combine($projectDir, "src", "Raven.Client",  "Raven.Client.xproj")) $(Join-Path $ravenClientDir -ChildPath  "Raven.Client.xproj")
}

function CopyNewClient ( $clientOutDir, $packageDir, $assetsDir ) {
    # layout client dir structure
    $ravenClientAssetsDir = [io.path]::combine($assetsDir, "Raven.NewClient")
    $ravenClientDir = [io.path]::combine($packageDir, 'Client', $NETSTANDARD16, 'Raven.NewClient')
    $ravenClientDllDir = [io.path]::combine($ravenClientDir, $NETSTANDARD16)
    New-Item -ItemType Directory -Path $ravenClientDllDir

    cp $(Join-Path $clientOutDir -ChildPath "Raven.NewClient.dll") $ravenClientDllDir
    cp $(Join-Path $clientOutDir -ChildPath "Raven.NewClient.pdb") $ravenClientDllDir

    $ravenClientProjectTemplate = Get-Content -Raw -Path $(Join-Path $ravenClientAssetsDir "project.json.template") | ConvertFrom-Json
    $ravenClientProjectOrig = Get-Content -Raw -Path $([io.path]::combine($projectDir, "src", "Raven.NewClient",  "project.json")) | ConvertFrom-Json
    $ravenClientProjectTemplate.dependencies = $ravenClientProjectOrig.dependencies
    $ravenClientProjectTemplate `
    | ConvertTo-Json -Depth 100 `
    | Out-File $(Join-Path $ravenClientDir -ChildPath "project.json") -Encoding UTF8

    cp $([io.path]::combine($projectDir, "src", "Raven.NewClient", "Raven.NewClient.xproj")) $(Join-Path $ravenClientDir -ChildPath  "Raven.NewClient.xproj")
}

function CopySparrow ( $clientOutDir, $packageDir, $assetsDir ) {
    $sparrowAssetsDir = [io.path]::combine($assetsDir, "Sparrow")
    # layout sparrow dir structure
    $sparrowDir = [io.path]::combine($packageDir, 'Client', $NETSTANDARD16, 'Sparrow')
    $sparrowDllDir = [io.path]::combine($packageDir, 'Client', $NETSTANDARD16, 'Sparrow', $NETSTANDARD16)
    New-Item -ItemType Directory -Path $sparrowDllDir

    cp $(Join-Path $clientOutDir -ChildPath "Sparrow.dll") $sparrowDllDir
    cp $(Join-Path $clientOutDir -ChildPath "Sparrow.pdb") $sparrowDllDir
    cp $([io.path]::combine($projectDir, "src", "Sparrow", "Sparrow.xproj")) $(Join-Path $sparrowDir -ChildPath  "Sparrow.xproj")
    cp $(Join-Path $sparrowAssetsDir -ChildPath "project.json.template") $(Join-Path $sparrowDir -ChildPath "project.json")
}
