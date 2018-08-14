function CopyLicenseFile ( $targetDir ) {
    $licensePath = "LICENSE"
    Copy-Item "$licensePath" -Destination "$targetDir"
}

function CopyAckFile ( $targetDir ) {
    $licensePath = [io.path]::combine("docs", "acknowledgements.txt")
    Copy-Item "$licensePath" -Destination "$targetDir"
}

function CopyServerReadmeFile ( $target, $targetDir ) {
    if ($target.IsUnix -eq $False) {
        $readmeFile = 'readme.windows.txt'
    } else {
        $readmeFile = 'readme.linux.txt'
    }

    $readmePath = Join-Path -Path "docs" -ChildPath $readmeFile
    $targetFile = Join-Path -Path $targetDir -ChildPath 'readme.txt'
    Copy-Item "$readmePath" -Destination "$targetFile"
}

function CopyServerStartScript ( $projectDir, $targetDir, $packOpts ) {
    if ($packOpts.Target.IsUnix -eq $False) {
        CopyStartCmd $projectDir $targetDir $packOpts
    } else {
        CopyStartSh $targetDir
    }
}

function CopyServerStartAsServiceScript ( $projectDir, $targetDir, $packOpts ) {
    if ($packOpts.Target.IsUnix -eq $False) {
        CopyStartAsServiceCmd $projectDir $targetDir $packOpts
    }
}

function CopyStartCmd ( $projectDir, $targetDir, $packOpts ) {
    $startPs1Path = [io.path]::combine("scripts", "assets", "run.ps1")
    $startPs1TargetPath = [io.path]::combine($targetDir, "run.ps1");
    write-host "Copy $startPs1Path -> $startPs1TargetPath"
    Copy-Item $startPs1Path $startPs1TargetPath

    if ($packOpts.VersionInfo.BuildType.ToLower() -ne 'custom') {
        write-host "Signing $startPs1TargetPath"
        SignFile $projectDir $startPs1TargetPath $packOpts.DryRunSign
    }

}

function CopyStartAsServiceCmd ( $projectDir, $targetDir, $packOpts ) {
    $startAsServicePs1Path = [io.path]::combine("scripts", "assets", "setup-as-service.ps1")
    $startAsServicePs1TargetPath = [io.path]::combine($targetDir, "setup-as-service.ps1");
    write-host "Copy $startAsServicePs1Path -> $startAsServicePs1TargetPath"
    Copy-Item $startAsServicePs1Path $startAsServicePs1TargetPath

    if ($packOpts.VersionInfo.BuildType.ToLower() -ne 'custom') {
        write-host "Signing $startAsServicePs1TargetPath"
        SignFile $projectDir $startAsServicePs1TargetPath $packOpts.DryRunSign
    }
}

function CopyStartSh ( $targetDir ) {
    $startPs1Path = [io.path]::combine("scripts", "assets", "run.sh")
    Copy-Item -Path $startPs1Path -Destination "$targetDir"
}
