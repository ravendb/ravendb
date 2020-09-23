function CreateNugetPackage ( $srcDir, $targetFilename, $versionSuffix ) {
    dotnet pack /p:GenerateDocumentationFile=true --output $targetFilename `
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

function BuildEmbeddedNuget ($projectDir, $outDir, $serverSrcDir, $studioZipPath, $debug) {
    $EMBEDDED_SRC_DIR = [io.path]::combine($projectDir, "src", "Raven.Embedded")
    
    $EMBEDDED_NUSPEC = [io.path]::combine($outDir, "RavenDB.Embedded", "RavenDB.Embedded.nuspec")
    $EMBEDDED_OUT_DIR = [io.path]::combine($outDir, "RavenDB.Embedded")
    $EMBEDDED_BUILD_OUT_DIR = [io.path]::combine($EMBEDDED_OUT_DIR, "build")
    $EMBEDDED_SERVER_OUT_DIR = [io.path]::combine($EMBEDDED_OUT_DIR, "contentFiles", "any", "any")
    
    $NETSTANDARD_TARGET = "netstandard2.0"
    $NET461_TARGET = "net461"
    
    $EMBEDDED_LIB_OUT_DIR_NETSTANDARD = [io.path]::combine($EMBEDDED_OUT_DIR, "lib", "$NETSTANDARD_TARGET")
    $EMBEDDED_LIB_OUT_DIR_NET461 = [io.path]::combine($EMBEDDED_OUT_DIR, "lib", "$NET461_TARGET")
    
    write-host "Preparing Raven.Embedded NuGet package.."
    $nuspec = [io.path]::combine($EMBEDDED_SRC_DIR, "Raven.Embedded.nuspec.template")
    & New-Item -ItemType Directory -Path $EMBEDDED_BUILD_OUT_DIR -Force
    & New-Item -ItemType Directory -Path $EMBEDDED_SERVER_OUT_DIR -Force
    & New-Item -ItemType Directory -Path $EMBEDDED_LIB_OUT_DIR_NETSTANDARD -Force
    & New-Item -ItemType Directory -Path $EMBEDDED_LIB_OUT_DIR_NET461 -Force

    Copy-Item $nuspec -Destination $EMBEDDED_NUSPEC

    $embeddedCsproj = Join-Path -Path $EMBEDDED_SRC_DIR -ChildPath "Raven.Embedded.csproj";
    
    BuildEmbedded $embeddedCsproj $EMBEDDED_LIB_OUT_DIR_NETSTANDARD $NETSTANDARD_TARGET
    Remove-Item $(Join-Path $EMBEDDED_LIB_OUT_DIR_NETSTANDARD -ChildPath "*") -Exclude "Raven.Embedded.dll","Raven.Embedded.xml"
    
    BuildEmbedded $embeddedCsproj $EMBEDDED_LIB_OUT_DIR_NET461 $NET461_TARGET
    Remove-Item $(Join-Path $EMBEDDED_LIB_OUT_DIR_NET461 -ChildPath "*") -Exclude "Raven.Embedded.dll","Raven.Embedded.xml"

    BuildServer $SERVER_SRC_DIR $EMBEDDED_SERVER_OUT_DIR $null $Debug
    $tempServerDir = Join-Path $EMBEDDED_SERVER_OUT_DIR -ChildPath "Server"
    $serverDir = Join-Path $EMBEDDED_SERVER_OUT_DIR -ChildPath "RavenDBServer"
    Write-Host "Move $tempServerDir -> $serverDir"
    Rename-Item $tempServerDir -NewName "RavenDBServer" 
    write-host "Remove settings.default.json"
    Remove-Item $(Join-Path $serverDir -ChildPath "settings.default.json")
    write-host "Copying Studio $studioZipPath -> $serverDir"
    Copy-Item "$studioZipPath" -Destination $serverDir
    
    $targetsSrc = [io.path]::combine($EMBEDDED_SRC_DIR, "RavenDB.Embedded.targets")
    $targetsDst = [io.path]::combine($EMBEDDED_BUILD_OUT_DIR, "RavenDB.Embedded.targets")
    Copy-Item "$targetsSrc" -Destination "$targetsDst"
    
    CopyLicenseFile($EMBEDDED_OUT_DIR);
    CopyIconFile($EMBEDDED_OUT_DIR);
    
    try {
        Push-Location $EMBEDDED_OUT_DIR
        & ../../scripts/assets/bin/nuget.exe pack .\RavenDB.Embedded.nuspec
        CheckLastExitCode
    } finally {
        Pop-Location
    }

    write-host "Raven.Embedded NuGet package in $OUT_DIR\Raven.Embedded.nupkg."
}
