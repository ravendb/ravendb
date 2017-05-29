
function BuildServer ( $srcDir, $outDir, $spec ) {
    if ($spec.TargetId -eq 'rpi') {
        BuildServerArm $srcDir $outDir $spec
    } else {
        BuildServerRegular $srcDir $outDir $spec
    }
}

function BuildServerRegular ( $srcDir, $outDir, $spec ) {
    write-host "Building Server for $specName..."

    $output = [io.path]::combine($outDir, "Server");
    if ([string]::IsNullOrEmpty($spec.Arch)) {
        & dotnet publish --output $output `
            --runtime $($spec.Runtime) `
            --configuration "Release" $srcDir;
    } else {
            & dotnet publish --output $output `
                --runtime $($spec.Runtime) `
                --configuration "Release" $srcDir `
                /p:Platform=$($spec.Arch);
    }

    CheckLastExitCode
}

function BuildServerArm ( $srcDir, $outDir, $spec ) {
    write-host "Building Server for $($spec.Name)"

    $output = [io.path]::combine($outDir, "Server");
    $bin = [io.path]::combine($srcDir, "bin");

    Remove-Item -Recurse -Force $bin

    & dotnet publish --output $output `
                 --configuration "Release" $srcDir `
                 /p:ARM=true
    CheckLastExitCode
}

function BuildClient ( $srcDir ) {
    write-host "Building Client"
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

        Write-Host "Update version.json..."
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