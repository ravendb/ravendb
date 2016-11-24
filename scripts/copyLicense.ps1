function CopyLicenseFile ( $serverOutDir ) {
    cp "docs\license.txt" $serverOutDir
}

function CopyAckFile ( $serverOutDir ) {
    cp "docs\acknowledgments.txt" $serverOutDir
}
