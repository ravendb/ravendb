
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

function CopyNativeBinariesRequiredForDebugging($buildOutputDir, $targetDir, $nativeBinExtension) {
    # When .NET does not require it anymore we can skip this copy step
    # https://github.com/dotnet/designs/blob/main/accepted/2020/single-file/design.md#host-builds
    Write-Host "Copy native binaries for debugging on POSIX."

    $nativeBinaries = @(
        [io.path]::combine($buildOutputDir, "libmscordaccore.$nativeBinExtension"),
        [io.path]::combine($buildOutputDir, "createdump")
    );

    foreach ($asset in $nativeBinaries) {
        if (!(Test-Path $asset)) {
            throw "Native binary file $asset does not exist."
        }

        $dst = [io.path]::combine($targetDir, $(Get-Item $asset).Name)
        write-host "Copy $asset -> $dst"
        Copy-Item $asset $dst
    }

}

function LayoutServerPackage ( $packageDir, $projectDir, $packOpts ) {
    $target = $packOpts.Target
    CopyStudioPackageToServerOutputDirectory $packOpts

    if ($target.IsUnix) {
        # TODO get 'net5.0' from somewhere
        $buildDir = [io.path]::Combine($projectDir, "src", "Raven.Server", "bin", "Release", "net5.0", $target.Runtime)
        CopyNativeBinariesRequiredForDebugging $buildDir "$($packOpts.OutDirs.Server)" $target.NativeBinExtension
    }

    CopyLicenseFile $packageDir
    CopyAckFile $packageDir
    CopyServerStartScript $projectDir $packageDir $packOpts
    CopyServerServiceScripts $projectDir $packageDir $packOpts
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
    write-host "Copy $tag files: $src -> $dst"
    Copy-FileHash -Path "$src" -Destination "$dst" -Recurse -ThrowForDlls
}

function CopyStudioPackageToServerOutputDirectory ( $packOpts ) {
    if ($packOpts.SkipCopyStudioPackage) {
        write-host "Skip copying Studio..."
        return;
    }

    $studioZipPath = [io.path]::combine($packOpts.OutDirs.Studio, "Raven.Studio.zip")
    $dst = $packOpts.OutDirs.Server
    if (-not (Test-Path $studioZipPath)) {
        throw "Studio ZIP package does not exist at $studioZipPath."
    }

    write-host "Copying Studio $studioZipPath -> $dst"
    Copy-Item "$studioZipPath" -Destination $dst
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
