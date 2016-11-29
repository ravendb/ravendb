function CopyLicenseFile ( $targetDir ) {
    $licensePath = [io.path]::combine("docs", "license.txt")
    cp $licensePath $targetDir
}

function CopyAckFile ( $targetDir ) {
    $licensePath = [io.path]::combine("docs", "acknowledgments.txt")
    cp $licensePath $targetDir
}

function CopyClientReadMe ( $targetDir ) {
    $licensePath = [io.path]::combine("docs", "readme.txt")
    cp $licensePath $targetDir
}
