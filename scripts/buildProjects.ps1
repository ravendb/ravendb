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

function BuildClient ( $srcDir, $outDir, $specName ) {
    write-host "Building Client for $specName..."

    & dotnet build --output $outDir `
                --no-incremental `
                --framework "netstandard1.3" `
                --configuration "Release" $srcDir;
    CheckLastExitCode
}

function BuildNewClient ( $srcDir, $outDir, $specName ) {
    write-host "Building NewClient for $specName..."

    & dotnet build --output $outDir `
                --no-incremental `
                --framework "netstandard1.3" `
                --configuration "Release" $srcDir;
    CheckLastExitCode
}

function BuildTypingsGenerator ( $srcDir ) {
    # build typings generator
    & dotnet build --configuration "Debug" $srcDir;
    CheckLastExitCode
}

function BuildSparrow ( $srcDir ) {
    # build sparrow
    & dotnet build --configuration "Release" $srcDir;
    CheckLastExitCode
}

function BuildStudio ( $srcDir, $projectDir, $version ) {
    # build studio
    write-host "Building Studio..."
    cd $srcDir

    & npm install
    CheckLastExitCode

    echo "Update version.json..."
    $versionJsonPath = [io.path]::combine($srcDir, "wwwroot", "version.json")
    "{ ""Version"": ""$version"" }" | Out-File $versionJsonPath -Encoding UTF8


    & npm run gulp alpha-release
    CheckLastExitCode

    cd $projectDir
}
