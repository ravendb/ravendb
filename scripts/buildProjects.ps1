function BuildServer ( $srcDir, $outDir, $runtime, $specName ) {
    write-host "Building Server for $specName..."
    #build server
    $output = [io.path]::combine($outDir, "Server");
    $build = [io.path]::combine($buildDir, $runtime)
    & dotnet publish --output $output `
                 --runtime $runtime `
                 --configuration "Release" $srcDir;
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

function BuildStudio ( $srcDir, $projectDir, $version ) {
    write-host "Building Studio..."
    cd $srcDir

    & npm install
    CheckLastExitCode

    echo "Update version.json..."
    $versionJsonPath = [io.path]::combine($srcDir, "wwwroot", "version.json")
    "{ ""Version"": ""$version"" }" | Out-File $versionJsonPath -Encoding UTF8


    & npm run gulp release
    CheckLastExitCode

    cd $projectDir
}
