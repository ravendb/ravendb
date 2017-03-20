
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
    $ORIGINAL_NETCOREAPP_DEP = "`"Microsoft.NETCore.App`": `"1.1.0`"";
    $NORUNTIME_NETCOREAPP_DEP = "`"Microsoft.NETCore.App`": { `"version`": `"1.1.0`", `"type`": `"platform`" }";
    $serverProjectJsonPath = [io.path]::combine($srcDir, "project.json")

    $content = (Get-Content $serverProjectJsonPath) |
        Foreach-Object { $_ -replace $ORIGINAL_NETCOREAPP_DEP, $NORUNTIME_NETCOREAPP_DEP }

        try
        {
            Set-Content -Path $serverProjectJsonPath -Value $content -Encoding UTF8

            & dotnet restore
            CheckLastExitCode

            $output = [io.path]::combine($outDir, "Server");
            & dotnet publish --output $output --configuration "Release" $srcDir;
            CheckLastExitCode
        }
        finally
        {
            $content = (Get-Content $serverProjectJsonPath) |
            Foreach-Object { $_ -replace $NORUNTIME_NETCOREAPP_DEP, $ORIGINAL_NETCOREAPP_DEP }
            Set-Content -Path $serverProjectJsonPath -Value $content -Encoding UTF8

            & dotnet restore
            CheckLastExitCode
        }
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
