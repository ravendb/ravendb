function BuildServer ( $srcDir, $outDir, $buildDir, $platform ) {
    write-host "Building Server for $platform..."
    #build server
    $output = [io.path]::combine($outDir, "Server");
    $build = [io.path]::combine($buildDir, $platform)
    & dotnet build --output $output `
                 --no-incremental `
                 --build-base-path $build `
                 --framework "netcoreapp1.1" `
                 --runtime $platform `
                 --configuration "Release" $srcDir;

    CheckLastExitCode
}

function BuildClient ( $srcDir, $outDir, $buildDir, $platform ) {
    write-host "Building Client for $platform..."
    # build client
    $output = [io.path]::combine($outDir, "Client");
    $build = [io.path]::combine($buildDir, $platform)
    & dotnet build --output $output `
                --no-incremental `
                --build-base-path $build `
                --framework "netstandard1.6" `
                --runtime $platform `
                --configuration "Release" $srcDir;
    CheckLastExitCode
}

function BuildTypingsGenerator ( $srcDir ) {
    # build typings generator
    & dotnet build --framework "netcoreapp1.1" `
        --configuration "Debug" $srcDir;
    CheckLastExitCode
}

function BuildStudio ( $srcDir, $projectDir, $version ) {
    # build studio
    write-host "Building Studio..."
    & npm install -g gulp-cli
    cd $srcDir

    & npm install
    CheckLastExitCode

    $versionJsonPath = [io.path]::combine($srcDir, "wwwroot", "version.json")
    echo "{ ""Version"": ""$version"" }" > $versionJsonPath
    echo "Update version.json..."

    & gulp alpha-release
    CheckLastExitCode

    cd $projectDir
}
