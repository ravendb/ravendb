function BuildServer ( $srcDir, $outDir, $buildDir, $platform ) {
    write-host "Building Server for $platform..."
    #build server
    & dotnet build --output "$outDir\Server" `
                 --no-incremental `
                 --build-base-path "$buildDir\$platform" `
                 --framework "netcoreapp1.1" `
                 --runtime $platform `
                 --configuration "Release" $srcDir;

    CheckLastExitCode
}

function BuildClient ( $srcDir, $outDir, $buildDir, $platform ) {
    write-host "Building Client for $platform..."
    # build client
    & dotnet build --output "$outDir\Client" `
                --no-incremental `
                --build-base-path "$buildDir\$platform" `
                --framework "netstandard1.6" `
                --runtime $platform `
                --configuration "Release" $srcDir;
    CheckLastExitCode
}

function BuildTypingsGenerator ( $srcDir ) {
    # build typings generator
    & dotnet build --framework "netcoreapp1.1" `
        --configuration "Debug" $srcDir;
}

function BuildStudio ( $srcDir, $projectDir, $version ) {
    # build studio
    write-host "Building Studio..."
    & npm install -g gulp-cli
    cd $srcDir

    & npm install
    CheckLastExitCode

    echo "{ ""Version"": ""$version"" }" > "$srcDir\wwwroot\version.json"
    echo "Update version.json..."

    & gulp alpha-release
    CheckLastExitCode

    cd $projectDir
}
