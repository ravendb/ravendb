function BuildServer ( $srcDir, $outDir, $buildDir ) {
    write-host "Building Server..."
    #build server
    & dotnet build --output "$outDir\Server" `
                 --build-base-path "$buildDir\Server" `
                 --framework "netcoreapp1.1" `
                 --configuration "Release" $srcDir;

    CheckLastExitCode
}

function BuildClient ( $srcDir, $outDir, $buildDir ) {
    write-host "Building Client..."
    # build client
    & dotnet build --output "$outDir\Client" `
                --build-base-path "$buildDir\Client" `
                --framework "netstandard1.6" `
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
