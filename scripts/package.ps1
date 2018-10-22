
function CreateServerPackage ( $projectDir, $releaseDir, $packOpts ) {
    $target = $packOpts.Target
    write-host "Create server package for $($target.runtime)..."
    $packageDir = [io.path]::combine($packOpts.outDirs.Main, "ServerPackage")
    New-Item -ItemType Directory -Path $packageDir | Out-Null

    LayoutServerPackage $packageDir $projectDir $packOpts

    $releaseArchiveFile = GetRavenArchiveFileName $packOpts.VersionInfo.Version $target
    $releaseArchivePath = [io.path]::combine($releaseDir, $releaseArchiveFile)
    if ($target.IsUnix) {
        CreateArchiveFromDir $releaseArchivePath $packageDir $target "RavenDB"
    } else {
        CreateArchiveFromDir $releaseArchivePath $packageDir $target
    }
    
    Remove-Item -Recurse -ErrorAction SilentlyContinue "$($packOpts.OutDirs.Server)"
    Remove-Item -Recurse -ErrorAction SilentlyContinue "$($packOpts.OutDirs.Rvn)"
    Remove-Item -Recurse -ErrorAction SilentlyContinue "$($packOpts.OutDirs.Debug)"
}

function CreateToolsPackage( $projectDir, $releaseDir, $packOpts ) {
    $target = $packOpts.Target
    write-host "Create tools package for $($target.runtime)..."
    $packageDir = [io.path]::combine($packOpts.outDirs.Main, "ToolsPackage")
    New-Item -ItemType Directory -Path $packageDir | Out-Null

    LayoutToolsPackage $packageDir $projectDir $packOpts

    $releaseArchiveFile = GetRavenArchiveFileName $packOpts.VersionInfo.Version $target "Tools"
    $releaseArchivePath = [io.path]::combine($releaseDir, $releaseArchiveFile)
    if ($target.IsUnix) {
        CreateArchiveFromDir $releaseArchivePath $packageDir $target "RavenDB-Tools"
    } else {
        CreateArchiveFromDir $releaseArchivePath $packageDir $target
    }
    
    Remove-Item -Recurse -ErrorAction SilentlyContinue "$($packOpts.OutDirs.Migrator)"
    Remove-Item -Recurse -ErrorAction SilentlyContinue "$($packOpts.OutDirs.Drtools)"
}

function GetRavenArchiveFileName ( $version, $target, $packageSuffix ) {
    $name = "RavenDB-$version-$($target.Name)"
    if ($packageSuffix) {
        $name = "$name.$packageSuffix"
    }

    return $name
}

function LayoutServerPackage ( $packageDir, $projectDir, $packOpts ) {
    $target = $packOpts.Target
    CopyStudioPackageToServerOutputDirectory $packOpts
    CopyLicenseFile $packageDir
    CopyAckFile $packageDir
    CopyServerStartScript $projectDir $packageDir $packOpts
    CopyServerStartAsServiceScript $projectDir $packageDir $packOpts
    CopyServerToolsToServerOutputDirectory $packOpts.OutDirs
    CopyServerReadmeFile $target $packageDir
    AddRuntimeTxt $projectDir $packageDir
    LayoutServerDirectory $projectDir $($packOpts.OutDirs.Server) $packageDir $target
}

function LayoutToolsPackage ( $packageDir, $projectDir, $packOpts ) {
    CopyLicenseFile $packageDir
    CopyAckFile $packageDir

    $toolsSubDir = Join-Path -Path $packageDir -ChildPath "Tools";
    New-Item -ItemType Directory -Path $toolsSubDir;

    CopyDirectoryContents "Raven.Migrator" $packOpts.OutDirs.Migrator $toolsSubDir
    CopyDirectoryContents "Voron.Recovery" $packOpts.OutDirs.Drtools $toolsSubDir
}

function CopyDirectoryContents ( $tag, $src, $dst ) {
    $contents = [io.path]::combine($src, "*")
    write-host "Copy $tag files: $contents -> $dst"
    Copy-Item -Recurse "$contents" -Destination "$dst" -Force 
}

function CopyStudioPackageToServerOutputDirectory ( $packOpts ) {
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

function CopyServerToolsToServerOutputDirectory ( $outDirs ) {
    CopyDirectoryContents "RVN" $outDirs.Rvn $outDirs.Server
    CopyDirectoryContents "Debug" $outDirs.Debug $outDirs.Server
}

function LayoutServerDirectory ( $projectDir, $serverOutDir, $packageDir, $target ) {
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
