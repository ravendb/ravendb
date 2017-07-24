$NETCORE_ARM_VERSION = "1.2.0-beta-001291-00"

function CreateRavenPackage ( $projectDir, $releaseDir, $outDirs, $spec, $version, $buildOptions ) {
    write-host "Create package for $($spec.runtime)..."
    $packageDir = [io.path]::combine($outDirs.Main, "package")
    New-Item -ItemType Directory -Path $packageDir | Out-Null

    CreatePackageLayout $packageDir $projectDir $outDirs $spec $buildOptions

    $releaseArchiveFile = GetRavenArchiveFileName $version $spec
    $releaseArchivePath = [io.path]::combine($releaseDir, $releaseArchiveFile)
    CreateArchiveFromDir $releaseArchivePath $packageDir $spec
    
    Remove-Item -Recurse -ErrorAction SilentlyContinue "$($outDirs.Server)"
    Remove-Item -Recurse -ErrorAction SilentlyContinue "$($outDirs.Rvn)"
}

function GetRavenArchiveFileName ( $version, $spec ) {
    "RavenDB-$version-$($spec.Name)"
}

function CreatePackageLayout ( $packageDir, $projectDir, $outDirs, $spec, $buildOptions ) {
    if ($spec.Name.Contains('raspberry-pi')) {
        LayoutRaspberryPiPackage $packageDir $projectDir $outDirs $spec $buildOptions
    } else {
        LayoutRegularPackage $packageDir $projectDir $outDirs $spec $buildOptions
    }
}

function LayoutRegularPackage ( $packageDir, $projectDir, $outDirs, $spec, $buildOptions ) {
    CopyStudioPackage $outDirs $buildOptions
    CopyLicenseFile $packageDir
    CopyAckFile $packageDir
    CopyStartScript $spec $packageDir
    CopyTools $outDirs
    CreatePackageServerLayout $projectDir $($outDirs.Server) $packageDir $spec
}

function LayoutRaspberryPiPackage ( $packageDir, $projectDir, $outDirs, $spec, $buildOptions ) {
    CopyStudioPackage $outDirs $buildOptions
    CopyLicenseFile $packageDir
    CopyAckFile $packageDir
    CopyRaspberryPiScripts $projectDir $packageDir
    IncludeDotnetForRaspberryPi $packageDir $outDirs
    CopyTools $outDirs
    CreatePackageServerLayout $projectDir $($outDirs.Server) $packageDir $spec
    WrapContentsInDir $packageDir "RavenDB.4.0"
}

function WrapContentsInDir ( $packageDir, $wrapperDirName ) {
    $wrapperDir = Join-Path $packageDir -ChildPath $wrapperDirName
    $rpiPkgContents = Get-ChildItem -Path $packageDir;
    New-Item -ItemType Directory -Path $wrapperDir | Out-Null

    Push-Location
    cd $packageDir
    foreach ($item in $rpiPkgContents) {
        Move-Item $item $wrapperDir
    }
    Pop-Location
}

function IncludeDotnetForRaspberryPi( $packageDir, $outDirs, $projectDir ) {
    $dotnetCacheDir = [system.io.path]::combine($projectDir, 'temp')
    DownloadDotnetRuntimeForUbuntu14Arm32 $packageDir $dotnetCacheDir
    $dotnetArchivePath = $(Join-Path -ChildPath "dotnet.tar.gz" $packageDir)
    $dotnetPath = $(Join-Path -ChildPath "dotnet" $packageDir)
    UnpackToDir  $dotnetArchivePath $dotnetPath
    Remove-Item $dotnetArchivePath

    $dllsToCopy = (
        'System.ComponentModel.Primitives.dll',
        'System.ComponentModel.TypeConverter.dll',
        'System.Collections.Specialized.dll'
    );

    write-host "Copy dotnet $NETCORE_ARM_VERSION DLLs to $($outDirs.Server)"
    foreach ($dll in $dllsToCopy) {
        $srcPath = [io.path]::combine($dotnetPath, "shared", "Microsoft.NETCore.App", $NETCORE_ARM_VERSION, $dll)
        $dstPath = [System.IO.Path]::Combine("$($outDirs.Server)", $dll)
        write-host "Copy $srcPath -> $dstPath"
        Copy-Item $srcPath -Destination $dstPath    
    }

    write-host "Copy dotnet $NETCORE_ARM_VERSION DLLs to $($outDirs.Rvn)"
    foreach ($dll in $dllsToCopy) {
        $srcPath = [io.path]::combine($dotnetPath, "shared", "Microsoft.NETCore.App", $NETCORE_ARM_VERSION, $dll)
        $dstPath = [System.IO.Path]::Combine("$($outDirs.Rvn)", $dll)
        write-host "Copy $srcPath -> $dstPath"
        Copy-Item $srcPath -Destination $dstPath    
    }
}

function CopyStudioPackage ( $outDirs, $buildOptions ) {
    if ($buildOptions.DontBuildStudio) {
        write-host "Skip copying Studio..."
        return;
    }

    $studioZipPath = [io.path]::combine($outDirs.Studio, "Raven.Studio.zip")
    $dst = $outDirs.Server
    write-host "Copying Studio $studioZipPath -> $dst"
    Copy-Item "$studioZipPath" -Destination $dst
    CheckLastExitCode
}

function CopyRaspberryPiScripts ( $projectDir, $packageDir ) {
    write-host "Copy RaspberryPi scripts..."

    $scriptsDir = [io.path]::combine($projectDir, "scripts", "raspberry-pi")
    $scriptsList = @( "run.sh" )

    Foreach ($scriptName in $scriptsList) {
        $scriptPath = Join-Path $scriptsDir -ChildPath $scriptName

        Copy-Item "$scriptPath" -Destination "$packageDir"

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
        Copy-Item "$scriptPath" -Destination "$packageDir"
    }
}

function CopyTools ( $outDirs ) {
    write-host "Copy rvn files..."
    $rvnContents = [io.path]::combine($outDirs.Rvn, "*")
    Copy-Item -Recurse "$rvnContents" -Destination "$($outDirs.Server)" -Force 
}

function CreatePackageServerLayout ( $projectDir, $serverOutDir, $packageDir, $spec ) {
    write-host "Create package server directory layout..."

    $settingsFileName = If ($spec.IsUnix) { "settings_posix.json" } Else { "settings_windows.json" }
    $settingsFilePath = [io.path]::combine($projectDir, 'src', 'Raven.Server', $settingsFileName)
    $settingsTargetPath = [io.path]::combine($serverOutDir, "settings.json")
    Copy-Item "$settingsFilePath" $settingsTargetPath

    Copy-Item "$serverOutDir" -Recurse -Destination "$packageDir"
}

function CreateRavenDBTarForRaspberryPi ( $projectDir, $packageDir ) {
    $targetFilename = "ravendb.4.0"

    $wrapperDir = Join-Path $packageDir -ChildPath $targetFilename
    New-Item -ItemType Directory -Path $wrapperDir

    Push-Location
    cd $packageDir

    $rpiRdbTarContents = ( "Server", "acknowledgements.txt", "license.txt", "ravendbd", "ravendb.watchdog.sh" )

    foreach ($item in $rpiRdbTarContents) {
        Move-Item $item $wrapperDir
    }

    if ($($IsWindows -eq $False) -and $(Get-Command "tar" -ErrorAction SilentlyContinue))
    {
        & tar -cjvf "$targetFilename.tar.bz2" "$targetFilename"
        CheckLastExitCode
    }
    else
    {
        $7za = [io.path]::combine($projectDir, "scripts", "assets", "bin", "7za.exe")
        & "$7za" a -ttar "$targetFilename.tar" "$targetFilename"
        CheckLastExitCode

        & "$7za" a -tbzip2 "$targetFilename.tar.bz2" "$targetFilename.tar"
        CheckLastExitCode

        Remove-Item "$targetFilename.tar"
    }

    Remove-Item -Recurse "$targetFilename"
    Pop-Location
}