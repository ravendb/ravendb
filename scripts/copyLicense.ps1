function CopyLicenseFile ( $serverOutDir ) {
    $licensePath = [io.path]::combine("docs", "license.txt")
    cp $licensePath $serverOutDir
}

function CopyAckFile ( $serverOutDir ) {
    $licensePath = [io.path]::combine("docs", "acknowledgments.txt")
    cp $licensePath $serverOutDir
}
