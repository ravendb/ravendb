function CopyLicenseFile ( $targetDir ) {
    $licensePath = [io.path]::combine("docs", "license.txt")
    Copy-Item "$licensePath" -Destination "$targetDir"
}

function CopyAckFile ( $targetDir ) {
    $licensePath = [io.path]::combine("docs", "acknowledgements.txt")
    Copy-Item "$licensePath" -Destination "$targetDir"
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
    $startPs1Path = [io.path]::combine("scripts", "assets", "start.ps1")
    $startPs1TargetPath = [io.path]::combine($targetDir, "start.ps1");
    Copy-Item $startPs1Path $startPs1TargetPath
    
    $startCmdContent = "@ECHO OFF`r`nPowerShell.exe -NoProfile -ExecutionPolicy Bypass -Command ""& '%~dp0\start.ps1'"" `r`n"
    $startCmdTargetPath = [io.path]::combine($targetDir, "start.cmd");
    Set-Content -Path $startCmdTargetPath $startCmdContent
}

function CopyStartAsServiceCmd ( $targetDir ) {
    $startAsServicePs1Path = [io.path]::combine("scripts", "assets", "start-as-service.ps1")
    $startAsServicePs1TargetPath = [io.path]::combine($targetDir, "start-as-service.ps1");
    Copy-Item $startAsServicePs1Path $startAsServicePs1TargetPath
    
    $startAsServiceCmdContent = "@ECHO OFF`r`nPowerShell.exe -NoProfile -ExecutionPolicy Bypass -Command ""& '%~dp0\start-as-service.ps1'"" `r`n"
    $startAsServiceCmdTargetPath = [io.path]::combine($targetDir, "start-as-service.cmd");
    Set-Content -Path $startAsServiceCmdTargetPath $startAsServiceCmdContent
}

function CopyStartSh ( $targetDir ) {
    $startPs1Path = [io.path]::combine("scripts", "assets", "start.sh")
    Copy-Item -Path $startPs1Path -Destination "$targetDir"
}
