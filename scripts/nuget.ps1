function CreateNugetPackage ( $srcDir,  $targetFilename, $versionSuffix ) {
    dotnet pack --output $targetFilename `
                --configuration "Release" `
                --version-suffix $versionSuffix `
                $srcDir
}
