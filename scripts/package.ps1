$NETSTANDARD13 = 'netstandard1.3'
$SUPPORTED_CLIENT_FRAMEWORKS = @( 'netstandard1.3' ) 
$SUPPORTED_NEW_CLIENT_FRAMEWORKS = @( 'net46', $NETSTANDARD13 ) 

function CreateArchiveFromDir ( $targetFilename, $dir, $spec ) {
    if ($spec.PkgType -eq "zip") {
        ZipFilesFromDir $targetFilename $dir
    } elseif ($spec.PkgType -eq "tar.bz2") {
        TarBzFilesFromDir $targetFilename $dir
    } elseif ($spec.PkgType -eq "tar") {
        TarFilesFromDir $targetFilename $dir
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

function TarBzFilesFromDir ( $targetFilename, $sourceDir ) {
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
        CheckLastExitCode
    }
}

function TarFilesFromDir ( $targetFilename, $sourceDir ) {
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
    #CopyStudioPackage $outDirs
    CopyLicenseFile $packageDir
    CopyAckFile $packageDir
    CreatePackageServerLayout $($outDirs.Server) $packageDir $projectDir $spec
    CreatePackageClientLayout $outDirs $packageDir $projectDir $spec

    if ($spec.Name.Contains('raspberry-pi') -eq $False) {
        CopyClientReadMe $(Join-Path $packageDir -ChildPath 'Client')
    } else {
        CopyDaemonScripts $projectDir $packageDir
        CopyLinuxScripts $projectDir $packageDir
        CopyDotnetTarForRaspberryPi $projectDir $packageDir
        CreateRavenDBTarForRaspberryPi $projectDir $packageDir
    }
}

function CopyStudioPackage ( $outDirs ) {
    $studioZipPath = [io.path]::combine($outDirs.Studio, "Raven.Studio.zip")
    write-host "Copying Studio package from $studioZipPath to $outDirs.Server"
    cp $studioZipPath $outDirs.Server
    CheckLastExitCode
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
    if ($spec.Name.Contains("raspberry-pi")) {
        CreateRaspberryPiClientLayout $outDirs $packageDir $projectDir
    } else {
        CreateRegularPackageClientLayout $outDirs $packageDir $projectDir
    }
}

function CreateRaspberryPiClientLayout ( $outDirs, $packageDir, $projectDir ) {
    $clientOutDir = [io.path]::combine($outDirs.Client, $NETSTANDARD16)
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

function CopyDotnetTarForRaspberryPi ( $projectDir, $packageDir ) {
    $dotnetTarPath = [io.path]::combine($projectDir, "scripts", "assets", "pkg", 'raspberry-pi', 'dotnet.tar.bz2')
    cp $dotnetTarPath $packageDir
}

function CreateRavenDBTarForRaspberryPi ( $projectDir, $packageDir ) {
    $targetFilename = "ravendb.4.0"
    Push-Location

    if ($(Get-Command "tar" -ErrorAction SilentlyContinue))
    {
        & tar -C $packageDir -cjvf "$targetFilename.tar.bz2" "Client" "Server" "acknowledgements.txt" "license.txt" "ravendbd" "ravendb.watchdog.sh"
        CheckLastExitCode
    }
    else
    {
        cd $packageDir
        ls 
        $7za = [io.path]::combine($projectDir, "scripts", "assets", "bin", "7za.exe")
        & "$7za" a -ttar "$targetFilename.tar" "Client" "Server" "acknowledgments.txt" "license.txt" "ravendbd" "ravendb.watchdog.sh"
        CheckLastExitCode

        & "$7za" a -tbzip2 "$targetFilename.tar.bz2" "$targetFilename.tar"
        CheckLastExitCode
    }

    Remove-Item "$targetFilename.tar"
    Remove-Item -Recurse "Client"
    Remove-Item -Recurse "Server"
    Remove-Item "acknowledgments.txt"
    Remove-Item "license.txt"
    Remove-Item "ravendbd"
    Remove-Item "ravendb.watchdog.sh"
    Pop-Location
}

function CreateRegularPackageClientLayout( $outDirs, $packageDir, $projectDir ) {
    write-host "Create package client directory layout..."

    $assetsDir = [io.path]::combine($projectDir, "scripts", "assets", "pkg")

    foreach ($framework in $SUPPORTED_CLIENT_FRAMEWORKS) {
        $frameworkClientOutDir = [io.path]::combine($outDirs.Client, $framework)
        CopyClient $frameworkClientOutDir $packageDir $assetsDir $framework
    }

    foreach ($framework in $SUPPORTED_NEW_CLIENT_FRAMEWORKS) {
        $frameworkNewClientOutDir = [io.path]::combine($outDirs.NewClient, $framework)
        CopyNewClient $frameworkNewClientOutDir $packageDir $assetsDir $framework
    }

    $sparrowFrameworks = $($SUPPORTED_CLIENT_FRAMEWORKS + $SUPPORTED_NEW_CLIENT_FRAMEWORKS) | select -uniq
    write-host $sparrowFrameworks
    foreach ($framework in $sparrowFrameworks) {
        $frameworkOutDir = [io.path]::combine($outDirs.Sparrow, $framework)
        CopySparrow $frameworkOutDir $packageDir $assetsDir $framework
    }
    
}

function CopyClient ( $clientOutDir, $packageDir, $assetsDir, $framework ) {
    $ravenClientAssetsDir = [io.path]::combine($assetsDir, "Raven.Client")
    $ravenClientDir = [io.path]::combine($packageDir, 'Client', $framework, 'Raven.Client')
    $ravenClientDllDir = [io.path]::combine($ravenClientDir, $framework)
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

function CopyNewClient ( $clientOutDir, $packageDir, $assetsDir, $framework ) {
    $ravenClientAssetsDir = [io.path]::combine($assetsDir, "Raven.NewClient")
    $ravenClientDir = [io.path]::combine($packageDir, 'Client', $framework, 'Raven.NewClient')
    $ravenClientDllDir = [io.path]::combine($ravenClientDir, $framework)
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

function CopySparrow ( $sparrowOutDir, $packageDir, $assetsDir, $framework ) {
    $sparrowAssetsDir = [io.path]::combine($assetsDir, "Sparrow")
    $sparrowDir = [io.path]::combine($packageDir, 'Client', $framework, 'Sparrow')
    $sparrowDllDir = [io.path]::combine($packageDir, 'Client', $framework, 'Sparrow', $framework)
    New-Item -ItemType Directory -Path $sparrowDllDir

    cp $(Join-Path $sparrowOutDir -ChildPath "Sparrow.dll") $sparrowDllDir
    cp $(Join-Path $sparrowOutDir -ChildPath "Sparrow.pdb") $sparrowDllDir
    cp $([io.path]::combine($projectDir, "src", "Sparrow", "Sparrow.xproj")) $(Join-Path $sparrowDir -ChildPath  "Sparrow.xproj")
    cp $(Join-Path $sparrowAssetsDir -ChildPath "project.json.template") $(Join-Path $sparrowDir -ChildPath "project.json")
}
