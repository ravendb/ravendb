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

function CopyStartCmd ( $targetDir ) {
    $startPs1Path = [io.path]::combine("scripts", "assets", "start.ps1")
    $startPs1 = Get-Content $startPs1Path
    $startPs1b64 = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($startPs1))
    $startCmdContent = "@ECHO OFF`r`nPowerShell.exe -EncodedCommand $startPs1b64`r`n"
    $startCmdTargetPath = [io.path]::combine($targetDir, "start.cmd");
    Set-Content -Path $startCmdTargetPath $startCmdContent
}

function CopyStartSh ( $targetDir ) {
    $startPs1Path = [io.path]::combine("scripts", "assets", "start.sh")
    Copy-Item -Path $startPs1Path -Destination "$targetDir"
}