function CopyLicenseFile ( $targetDir ) {
    $licensePath = "LICENSE"
    Copy-Item "$licensePath" -Destination "$targetDir"
}

function CopyAckFile ( $targetDir ) {
    $licensePath = [io.path]::combine("docs", "acknowledgements.txt")
    Copy-Item "$licensePath" -Destination "$targetDir"
}

function CopyReadmeFile ( $spec, $targetDir ) {
    if ($spec.IsUnix -eq $False) {
        $readmeFile = 'readme.windows.txt'
    } else {
        $readmeFile = 'readme.linux.txt'
    }

    $readmePath = Join-Path -Path "docs" -ChildPath $readmeFile
    $targetFile = Join-Path -Path $targetDir -ChildPath 'readme.txt'
    Copy-Item "$readmePath" -Destination "$targetFile"
}

function CopyStartScript ( $spec, $targetDir ) {
    if ($spec.IsUnix -eq $False) {
        CopyStartCmd $targetDir
    } else {
        CopyStartSh $targetDir
    }
}

function CopyStartAsServiceScript ( $spec, $targetDir ) {
    if ($spec.IsUnix -eq $False) {
        CopyStartAsServiceCmd $targetDir
    }
}

function CopyStartCmd ( $targetDir ) {
    $startPs1Path = [io.path]::combine("scripts", "assets", "run.ps1")
    $startPs1TargetPath = [io.path]::combine($targetDir, "run.ps1");
    Copy-Item $startPs1Path $startPs1TargetPath
}

function CopyStartAsServiceCmd ( $targetDir ) {
    $startAsServicePs1Path = [io.path]::combine("scripts", "assets", "setup-as-service.ps1")
    $startAsServicePs1TargetPath = [io.path]::combine($targetDir, "setup-as-service.ps1");
    Copy-Item $startAsServicePs1Path $startAsServicePs1TargetPath
}

function CopyStartSh ( $targetDir ) {
    $startPs1Path = [io.path]::combine("scripts", "assets", "start.sh")
    Copy-Item -Path $startPs1Path -Destination "$targetDir"
}
