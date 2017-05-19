
function BuildServer ( $srcDir, $outDir, $runtime, $specName ) {
    if ($specName -ne 'raspberry-pi') {
        BuildServerRegular $srcDir $outDir $runtime $specName
    } else {
        BuildServerArm $srcDir $outDir $runtime $specName
    }
}

function BuildServerRegular ( $srcDir, $outDir, $runtime, $specName ) {
    write-host "Building Server for $specName..."
    #build server
    $output = [io.path]::combine($outDir, "Server");
    $build = [io.path]::combine($buildDir, $runtime)
    & dotnet publish --output $output `
                 --runtime $runtime `
                 --configuration "Release" $srcDir;
    CheckLastExitCode
}

function BuildServerArm ( $srcDir, $outDir, $runtime, $specName ) {
    write-host "Building Server for $specName"
    #build server
    $output = [io.path]::combine($outDir, "Server");
    $build = [io.path]::combine($buildDir, $runtime);
    $bin = [io.path]::combine($srcDir, "bin");

    Remove-Item -Recurse -Force $bin

    & dotnet publish --output $output `
                 --configuration "Release" $srcDir `
                 /p:ARM=true
    CheckLastExitCode
}

function BuildClient ( $srcDir, $specName ) {
    write-host "Building Client for $specName..."
    & dotnet build --no-incremental `
                --configuration "Release" $srcDir;
    CheckLastExitCode
}

function BuildTypingsGenerator ( $srcDir ) {
    & dotnet build --no-incremental --configuration "Release" $srcDir;
    CheckLastExitCode
}

function BuildSparrow ( $srcDir ) {
    & dotnet build --configuration "Release" $srcDir;
    CheckLastExitCode
}

function BuildStudio ( $srcDir, $version ) {
    write-host "Building Studio..."

    Push-Location

    try {
        Set-Location $srcDir

        & npm install
        CheckLastExitCode

        echo "Update version.json..."
        $versionJsonPath = [io.path]::combine($srcDir, "wwwroot", "version.json")
        "{ ""Version"": ""$version"" }" | Out-File $versionJsonPath -Encoding UTF8

        & npm run gulp release
        CheckLastExitCode
    } 
    finally {
        Pop-Location
    }
}

function ShouldBuildStudio( $studioOutDir, $dontRebuildStudio ) {
    $studioZipPath = [io.path]::combine($studioOutDir, "Raven.Studio.zip")
    if (Test-Path $studioZipPath) {
        return ! $dontRebuildStudio
    }

    return $true
}