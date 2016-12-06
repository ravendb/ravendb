function CreateNugetPackage ( $srcDir,  $targetFilename, $versionSuffix ) {
    dotnet pack --output $targetFilename `
                --version-suffix $versionSuffix `
                --no-build `
                $srcDir
}
