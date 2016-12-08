function CopyLicenseFile ( $targetDir ) {
    $licensePath = [io.path]::combine("docs", "license.txt")
    Copy-Item "$licensePath" -Destination "$targetDir"
}

function CopyAckFile ( $targetDir ) {
    $licensePath = [io.path]::combine("docs", "acknowledgements.txt")
    Copy-Item "$licensePath" -Destination "$targetDir"
}

function CopyClientReadMe ( $targetDir ) {
    $licensePath = [io.path]::combine("docs", "readme.txt")
    Copy-Item "$licensePath" -Destination "$targetDir"
}
