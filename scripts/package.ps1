
function CreateRavenPackage ( $projectDir, $releaseDir, $packOpts ) {
    $target = $packOpts.Target
    write-host "Create package for $($target.runtime)..."
    $packageDir = [io.path]::combine($packOpts.outDirs.Main, "package")
    New-Item -ItemType Directory -Path $packageDir | Out-Null

    CreatePackageLayout $packageDir $projectDir $packOpts

    $releaseArchiveFile = GetRavenArchiveFileName $packOpts.VersionInfo.Version $target
    $releaseArchivePath = [io.path]::combine($releaseDir, $releaseArchiveFile)
    if ($target.IsUnix) {
        CreateArchiveFromDir $releaseArchivePath $packageDir $target "RavenDB"
    } else {
        CreateArchiveFromDir $releaseArchivePath $packageDir $target
    }
    
    Remove-Item -Recurse -ErrorAction SilentlyContinue "$($packOpts.OutDirs.Server)"
    Remove-Item -Recurse -ErrorAction SilentlyContinue "$($packOpts.OutDirs.Rvn)"
    Remove-Item -Recurse -ErrorAction SilentlyContinue "$($packOpts.OutDirs.Drtools)"
}

function GetRavenArchiveFileName ( $version, $target ) {
    "RavenDB-$version-$($target.Name)"
}

function CreatePackageLayout ( $packageDir, $projectDir, $packOpts ) {
    LayoutRegularPackage $packageDir $projectDir $packOpts
}

function LayoutRegularPackage ( $packageDir, $projectDir, $packOpts ) {
    $target = $packOpts.Target
    CopyStudioPackage $packOpts
    CopyLicenseFile $packageDir
    CopyAckFile $packageDir
    CopyStartScript $projectDir $packageDir $packOpts
    CopyStartAsServiceScript $projectDir $packageDir $packOpts
    CopyTools $packOpts.OutDirs
    CopyReadmeFile $target $packageDir
    AddRuntimeTxt $projectDir $packageDir
    CreatePackageServerLayout $projectDir $($packOpts.OutDirs.Server) $packageDir $target
}
function CopyStudioPackage ( $packOpts ) {
    if ($packOpts.SkipCopyStudioPackage) {
        write-host "Skip copying Studio..."
        return;
    }

    $studioZipPath = [io.path]::combine($packOpts.OutDirs.Studio, "Raven.Studio.zip")
    $dst = $packOpts.OutDirs.Server
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

function CreatePackageServerLayout ( $projectDir, $serverOutDir, $packageDir, $target ) {
    write-host "Create package server directory layout..."

    $settingsTargetPath = [io.path]::combine($serverOutDir, "settings.default.json")

    if ($target.IsUnix) { 
        $settingsFileName = "settings.posix.json" 
     } else { 
        $settingsFileName = "settings.windows.json" 
     }
 
     $settingsFilePath = [io.path]::combine($projectDir, "src", "Raven.Server", "Properties", "Settings", $settingsFileName)
    write-host "Copy $settingsFilePath -> $settingsTargetPath"
    Copy-Item -Force "$settingsFilePath" $settingsTargetPath
    
    Copy-Item "$serverOutDir" -Recurse -Destination "$packageDir"
}
