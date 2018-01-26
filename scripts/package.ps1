
function CreateRavenPackage ( $projectDir, $releaseDir, $outDirs, $spec, $version, $buildOptions ) {
    write-host "Create package for $($spec.runtime)..."
    $packageDir = [io.path]::combine($outDirs.Main, "package")
    New-Item -ItemType Directory -Path $packageDir | Out-Null

    CreatePackageLayout $packageDir $projectDir $outDirs $spec $buildOptions

    $releaseArchiveFile = GetRavenArchiveFileName $version $spec
    $releaseArchivePath = [io.path]::combine($releaseDir, $releaseArchiveFile)
    if ($spec.IsUnix) {
        CreateArchiveFromDir $releaseArchivePath $packageDir $spec "RavenDB"
    } else {
        CreateArchiveFromDir $releaseArchivePath $packageDir $spec
    }
    
    Remove-Item -Recurse -ErrorAction SilentlyContinue "$($outDirs.Server)"
    Remove-Item -Recurse -ErrorAction SilentlyContinue "$($outDirs.Rvn)"
    Remove-Item -Recurse -ErrorAction SilentlyContinue "$($outDirs.Drtools)"
}

function GetRavenArchiveFileName ( $version, $spec ) {
    "RavenDB-$version-$($spec.Name)"
}

function CreatePackageLayout ( $packageDir, $projectDir, $outDirs, $spec, $buildOptions ) {
    LayoutRegularPackage $packageDir $projectDir $outDirs $spec $buildOptions
}

function LayoutRegularPackage ( $packageDir, $projectDir, $outDirs, $spec, $buildOptions ) {
    CopyStudioPackage $outDirs $buildOptions
    CopyLicenseFile $packageDir
    CopyAckFile $packageDir
    CopyStartScript $spec $packageDir
    CopyStartAsServiceScript $spec $packageDir
    CopyTools $outDirs
    CopyReadmeFile $spec $packageDir
    CreatePackageServerLayout $projectDir $($outDirs.Server) $packageDir $spec
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
    $rvnContents = [io.path]::combine($outDirs.Rvn, "*")
    write-host "Copy rvn files: $rvnContents -> $($outDirs.Server)"
    Copy-Item -Recurse "$rvnContents" -Destination "$($outDirs.Server)" -Force 

    $drtoolsContents = [io.path]::combine($outDirs.Drtools, "*")
    write-host "Copy Voron.Recovery files: $drToolsContents -> $($outDirs.Server)"
    Copy-Item -Recurse "$drtoolsContents" -Destination "$($outDirs.Server)" -Force 
}

function CreatePackageServerLayout ( $projectDir, $serverOutDir, $packageDir, $spec ) {
    write-host "Create package server directory layout..."

    $settingsTargetPath = [io.path]::combine($serverOutDir, "settings.json")

    if ($spec.IsUnix) { 
        $settingsFileName = "settings.posix.json" 
     } else { 
        $settingsFileName = "settings.windows.json" 
     }
 
     $settingsFilePath = [io.path]::combine($projectDir, "src", "Raven.Server", "Properties", "Settings", $settingsFileName)
    write-host "Copy $settingsFilePath -> $settingsTargetPath"
    Copy-Item -Force "$settingsFilePath" $settingsTargetPath
    
    Copy-Item "$serverOutDir" -Recurse -Destination "$packageDir"
}
