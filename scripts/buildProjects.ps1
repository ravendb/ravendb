function BuildDummyApp ($outDir, $target) {

    if ($target) {
        write-host "Building Dummy App for $($target.Name)..."
    } else {
        write-host "Building Dummy App no specific target..."
    }

    $output = [io.path]::combine($outDir, "DummyApp");
    $quotedOutput = '"' + $output + '"'
    New-Item -Path $output -ItemType "Directory"

    Push-Location

    try {
        Set-Location $output

        Invoke-Expression -Command "dotnet new console"

        $command = "dotnet" 
        $commandArgs = @( "publish" )

        
        $commandArgs += @( "--output", $quotedOutput )

        $configuration = if ($global:isPublishConfigurationDebug) { 'Debug' } else { 'Release' }
        $commandArgs += @( "--configuration", $configuration )

        if ($target) {
            $commandArgs += $( "--runtime", "$($target.Runtime)" )
            $commandArgs += $( "--self-contained", "true" )
        }

        if ($target -and [string]::IsNullOrEmpty($target.Arch) -eq $false) {
            $commandArgs += "/p:Platform=$($target.Arch)"
        }

        write-host -ForegroundColor Cyan "Publish Dummy App: $command $commandArgs"
        Invoke-Expression -Command "$command $commandArgs"
        CheckLastExitCode
    } 
    finally {
        [Console]::ResetColor()
        Pop-Location
    }
} 

function BuildServer ( $srcDir, $outDir, $target) {

    if ($target) {
        write-host "Building Server for $($target.Name)..."
    } else {
        write-host "Building Server no specific target..."
    }

    $command = "dotnet" 
    $commandArgs = @( "publish" )

    $output = [io.path]::combine($outDir, "Server");
    $quotedOutput = '"' + $output + '"'
    $commandArgs += @( "--output", $quotedOutput )

    $configuration = if ($global:isPublishConfigurationDebug) { 'Debug' } else { 'Release' }
    $commandArgs += @( "--configuration", $configuration )
    
    if ($target) {
        $commandArgs += $( "--runtime", "$($target.Runtime)" )
        $commandArgs += $( "--self-contained", "true" )
    }

    $commandArgs += "$srcDir"

    if ($target -and [string]::IsNullOrEmpty($target.Arch) -eq $false) {
        $commandArgs += "/p:Platform=$($target.Arch)"
    }

    if (!$target) {
        $commandArgs += "/p:UseAppHost=false"
    }

    $commandArgs += '/p:SourceLinkCreate=true'
    
    if ($target -and $global:isPublishBundlingEnabled) {
        $commandArgs += '/p:PublishSingleFile=true /p:IncludeNativeLibrariesForSelfExtract=true /p:Client_IncludeZstd=false'
    }

    if ($env:RAVEN_IS_RUNNING_ON_CI){
        $commandArgs += '/p:ContinuousIntegrationBuild=true'
    }

    write-host -ForegroundColor Cyan "Publish server: $command $commandArgs"
    Invoke-Expression -Command "$command $commandArgs"
    CheckLastExitCode
}

function BuildClient ( $srcDir ) {
    $command = "dotnet" 
    $commandArgs = @( "build" )

    $commandArgs += "/p:SourceLinkCreate=true"
    $commandArgs += "/p:GenerateDocumentationFile=true"
    $commandArgs += "--no-incremental"
    $commandArgs += @( "--configuration", "Release" )

    if ($env:RAVEN_IS_RUNNING_ON_CI){
        $commandArgs += '/p:ContinuousIntegrationBuild=true'
    }
    
    $commandArgs += "$srcDir"

    write-host -ForegroundColor Cyan "Building Client: $command $commandArgs"
    Invoke-Expression -Command "$command $commandArgs"
    CheckLastExitCode
}

function BuildTestDriver ( $srcDir ) {
    $command = "dotnet" 
    $commandArgs = @( "build" )

    $commandArgs += "/p:SourceLinkCreate=true"
    $commandArgs += "/p:GenerateDocumentationFile=true"
    $commandArgs += "--no-incremental"
    $commandArgs += @( "--configuration", "Release" )

    if ($env:RAVEN_IS_RUNNING_ON_CI){
        $commandArgs += '/p:ContinuousIntegrationBuild=true'
    }
    
    $commandArgs += "$srcDir"

    write-host -ForegroundColor Cyan "Building TestDriver: $command $commandArgs"
    Invoke-Expression -Command "$command $commandArgs"
    CheckLastExitCode
}

function BuildTypingsGenerator ( $srcDir ) {
    $command = "dotnet" 
    $commandArgs = @( "build" )
    $commandArgs += "--no-incremental"

    $commandArgs += @( "--configuration", "Release" )

    if ($env:RAVEN_IS_RUNNING_ON_CI){
        $commandArgs += '/p:ContinuousIntegrationBuild=true'
    }
    
    $commandArgs += "$srcDir"

    write-host -ForegroundColor Cyan "Building TypingsGenerator: $command $commandArgs"
    Invoke-Expression -Command "$command $commandArgs"
    CheckLastExitCode
}

function BuildSparrow ( $srcDir ) {
    $command = "dotnet" 
    $commandArgs = @( "build" )

    $commandArgs += "/p:SourceLinkCreate=true"
    $commandArgs += "/p:GenerateDocumentationFile=true"
    $commandArgs += "--no-incremental"
    $commandArgs += @( "--configuration", "Release" )

    if ($env:RAVEN_IS_RUNNING_ON_CI){
        $commandArgs += '/p:ContinuousIntegrationBuild=true'
    }
    
    $commandArgs += "$srcDir"

    write-host -ForegroundColor Cyan "Building Sparrow: $command $commandArgs"
    Invoke-Expression -Command "$command $commandArgs"
    CheckLastExitCode
}

function NpmInstall () {
    write-host "Doing npm install..."

    foreach ($i in 1..3) {
        try {
            exec { npm install }
            CheckLastExitCode
            return
        }
        catch {
            write-host "Error doing npm install... Retrying."
        }
    }

    throw "npm install failed. Please see error above."
}

function BuildStudio ( $srcDir, $version ) {
    write-host "Building Studio..."

    Push-Location

    try {
        Set-Location $srcDir

        NpmInstall

        Write-Host "Update version.txt..."
        $versionJsonPath = [io.path]::combine($srcDir, "wwwroot", "version.txt")

        "{ ""Version"": ""$version"" }" | Out-File $versionJsonPath -Encoding ASCII

        exec { npm run release }
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

function BuildTool ( $toolName, $srcDir, $outDir, $target ) {
    write-host "Building $toolName for $($target.Name)..."
    $command = "dotnet" 
    $commandArgs = @( "publish" )

    $output = [io.path]::combine($outDir, "${toolName}");
    $quotedOutput = '"' + $output + '"'
    $commandArgs += @( "--output", $quotedOutput )
    $configuration = if ($debug) { 'Debug' } else { 'Release' }
    $commandArgs += @( "--configuration", $configuration )
    $commandArgs += $( "--runtime", "$($target.Runtime)" )
    $commandArgs += $( "--self-contained", "true" )
    $commandArgs += "$srcDir"

    if ([string]::IsNullOrEmpty($target.Arch) -eq $false) {
        $commandArgs += "/p:Platform=$($target.Arch)"
    }

    $commandArgs += "/p:SourceLinkCreate=true"
    
    if ($target -and $global:isPublishBundlingEnabled) {
        $commandArgs += "/p:PublishSingleFile=true /p:IncludeNativeLibrariesForSelfExtract=true /p:Client_IncludeZstd=false"
    }

    if ($env:RAVEN_IS_RUNNING_ON_CI){
        $commandArgs += '/p:ContinuousIntegrationBuild=true'
    }

    write-host -ForegroundColor Cyan "Publish ${toolName}: $command $commandArgs"
    Invoke-Expression -Command "$command $commandArgs"
    CheckLastExitCode
}

function BuildDebug ( $srcDir, $outDir, $target ) {
    BuildTool debug $srcDir $outDir $target

    if ($target -and $global:isPublishBundlingEnabled) {
        # workaround for https://github.com/microsoft/perfview/issues/2035
        
        $output = [io.path]::combine($outDir, "debug");
        $amd64 = [io.path]::combine($output, "amd64");
        $arm64 = [io.path]::combine($output, "arm64");
        $x86 = [io.path]::combine($output, "x86");

        Write-Host "Removing $amd64"
        Remove-Item $amd64 -ErrorAction SilentlyContinue -Recurse

        Write-Host "Removing $arm64"
        Remove-Item $arm64 -ErrorAction SilentlyContinue -Recurse

        Write-Host "Removing $x86"
        Remove-Item $x86 -ErrorAction SilentlyContinue -Recurse
    }
}

function BuildEmbedded ( $srcDir, $outDir, $framework) {
    $command = "dotnet" 
    $commandArgs = @( "build" )

    $commandArgs += "/p:GenerateDocumentationFile=true"
    $commandArgs += "/p:Client_IncludeZstd=false"
    $commandArgs += "--no-incremental"
    $commandArgs += @( "--output", $outDir )
    $commandArgs += @( "--framework", $framework )
    $commandArgs += @( "--configuration", "Release" )

    if ($env:RAVEN_IS_RUNNING_ON_CI){
        $commandArgs += '/p:ContinuousIntegrationBuild=true'
    }
    
    $commandArgs += "$srcDir"

    write-host -ForegroundColor Cyan "Building Embedded: $command $commandArgs"
    Invoke-Expression -Command "$command $commandArgs"
    CheckLastExitCode
}
