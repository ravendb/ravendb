function BuildServer ( $srcDir, $outDir, $spec ) {
    write-host "Building Server for $($spec.Name)..."
    $command = "dotnet" 
    $commandArgs = @( "publish" )

    $output = [io.path]::combine($outDir, "Server");
    $commandArgs += @( "--output", $output )
    $commandArgs += @( "--configuration", "Release" )
    
    if ($spec.TargetId -ne 'rpi') {
        $commandArgs += $( "--runtime", "$($spec.Runtime)" )
    }

    $commandArgs += "$srcDir"

    if ($spec.TargetId -eq "rpi") {
        $bin = [io.path]::combine($srcDir, "bin");
        write-host "Clean $bin"
        Remove-Item -Recurse -Force $bin

        $commandArgs += "/p:ARM=true"
    }

    if ([string]::IsNullOrEmpty($spec.Arch) -eq $false) {
        $commandArgs += "/p:Platform=$($spec.Arch)"
    }

    write-host -ForegroundColor Cyan "Publish server: $command $commandArgs"
    Invoke-Expression -Command "$command $commandArgs"
    CheckLastExitCode
}

function BuildClient ( $srcDir ) {
    write-host "Building Client"
    & dotnet build --no-incremental `
                --configuration "Release" $srcDir;
    CheckLastExitCode
}

function BuildTestDriver ( $srcDir ) {
    write-host "Building TestDriver"
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
        [Console]::ResetColor()
        Pop-Location
    }
}

function ShouldBuildStudio( $studioOutDir, $dontRebuildStudio, $dontBuildStudio ) {
    if ($dontBuildStudio) {
        return $false
    }

    $studioZipPath = [io.path]::combine($studioOutDir, "Raven.Studio.zip")
    if (Test-Path $studioZipPath) {
        return ! $dontRebuildStudio
    }

    return $true
}

function BuildRvn ( $srcDir, $outDir, $spec ) {
    write-host "Building Rvn for $($spec.Name)..."
    $command = "dotnet" 
    $commandArgs = @( "publish" )

    $output = [io.path]::combine($outDir, "rvn");
    $commandArgs += @( "--output", $output )
    $commandArgs += @( "--configuration", "Release" )
    
    if ($spec.TargetId -ne 'rpi') {
        $commandArgs += $( "--runtime", "$($spec.Runtime)" )
    }

    $commandArgs += "$srcDir"

    if ($spec.TargetId -eq "rpi") {
        $bin = [io.path]::combine($srcDir, "bin");
        Remove-Item -Recurse -Force $bin

        $commandArgs += "/p:ARM=true"
    }

    if ([string]::IsNullOrEmpty($spec.Arch) -eq $false) {
        $commandArgs += "/p:Platform=$($spec.Arch)"
    }

    write-host -ForegroundColor Cyan "Publish rvn: $command $commandArgs"
    Invoke-Expression -Command "$command $commandArgs"
    CheckLastExitCode
}