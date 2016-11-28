function BuildServer ( $srcDir, $outDir, $platform ) {
    write-host "Building Server for $platform..."
    #build server
    $output = [io.path]::combine($outDir, "Server");
    $build = [io.path]::combine($buildDir, $platform)
    & dotnet publish --output $output `
                 --runtime $platform `
                 --configuration "Release" $srcDir;
    CheckLastExitCode
}

function BuildClient ( $srcDir, $outDir ) {
    write-host "Building Client for $platform..."
    # build client
    $output = [io.path]::combine($outDir, "Client");
    $build = [io.path]::combine($buildDir, $platform)
    & dotnet build --output $output `
                --no-incremental `
                --framework "netstandard1.6" `
                --configuration "Release" $srcDir;
    CheckLastExitCode
}

function BuildTypingsGenerator ( $srcDir ) {
    # build typings generator
    & dotnet build --configuration "Debug" $srcDir;
    CheckLastExitCode
}

function BuildStudio ( $srcDir, $projectDir, $version ) {
    # build studio
    write-host "Building Studio..."
    & npm install -g gulp-cli
    cd $srcDir

    & npm install
    CheckLastExitCode

    echo "Update version.json..."
    $versionJsonPath = [io.path]::combine($srcDir, "wwwroot", "version.json")
    "{ ""Version"": ""$version"" }" | Out-File $versionJsonPath -Encoding UTF8


    & gulp alpha-release
    CheckLastExitCode

    cd $projectDir
}
