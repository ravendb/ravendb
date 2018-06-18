function CreateNugetPackage ( $srcDir, $targetFilename, $versionSuffix ) {
    dotnet pack --output $targetFilename `
        --configuration "Release" `
        --version-suffix $versionSuffix `
        $srcDir

    CheckLastExitCode
}

function ValidateClientDependencies ( $clientSrcDir, $sparrowSrcDir ) {
    $clientCsprojPath = Join-Path -Path $clientSrcDir -ChildPath "Raven.Client.csproj"
    $clientCsprojXml = [xml]$(Get-Content -Path $clientCsprojPath)
    
    $sparrowCsprojPath = Join-Path -Path $sparrowSrcDir -ChildPath "Sparrow.csproj"
    $sparrowCsprojXml = [xml]$(Get-Content -Path $sparrowCsprojPath)

    $clientDeps = $clientCsprojXml.selectNodes('//PackageReference').Include
    $sparrowDeps = $sparrowCsprojXml.selectNodes('//PackageReference').Include

    $missingSparrowDepsOnClient = @();
    foreach ($dep in $sparrowDeps) {
        if ($clientDeps -Contains $dep) {
            continue;
        }

        $missingSparrowDepsOnClient += $dep;
    }

    if ($missingSparrowDepsOnClient.Length -gt 0) {
        throw "Since we embed Sparrow.dll in Client nuget package we need to include its dependencies in Raven.Client.csproj. Add missing package references to Raven.Client.csproj: $missingSparrowDepsOnClient."
    }
}
