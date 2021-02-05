function UpdateSourceWithBuildInfo ( $projectDir, $buildNumber, $version ) {
    $commit = Get-Git-Commit-Short
    UpdateCommonAssemblyInfo $projectDir $buildNumber $version $commit

    $versionInfoFile = [io.path]::combine($projectDir, "src", "Raven.Client", "Properties", "VersionInfo.cs")
    UpdateRavenVersion $projectDir $buildNumber $version $commit $versionInfoFile

    UpdateCsprojAndNuspecWithVersionInfo $projectDir $version
}

function UpdateCsprojAndNuspecWithVersionInfo ( $projectDir, $version ) {
    # This is a workaround for the following issue:
    # dotnet pack - version suffix missing from ProjectReference: https://github.com/NuGet/Home/issues/4337

    Write-Host "Set version in Directory.build.props..."

    $src = $(Join-Path $projectDir -ChildPath "src");
    $testDriverCsproj = [io.path]::Combine($src, "Raven.TestDriver", "Raven.TestDriver.csproj")
    $clientCsproj = [io.path]::Combine($src, "Raven.Client", "Raven.Client.csproj")
    $embeddedCsproj = [io.path]::Combine($src, "Raven.Embedded", "Raven.Embedded.csproj")

    # https://github.com/Microsoft/msbuild/issues/1721
    UpdateVersionInFile $testDriverCsproj $version
    UpdateVersionInFile $clientCsproj $version
    UpdateVersionInFile $embeddedCsproj $version

    UpdateDirectoryBuildProps $projectDir "bench" $version
    UpdateDirectoryBuildProps $projectDir "src" $version
    UpdateDirectoryBuildProps $projectDir "test" $version
    UpdateDirectoryBuildProps $projectDir "tools" $version

    $embeddedNuspec = [io.path]::Combine($src, "Raven.Embedded", "Raven.Embedded.nuspec.template")
    UpdateVersionInNuspec $embeddedNuspec $version
}

function UpdateDirectoryBuildProps( $projectDir, $subDir, $version ) {
    $subDirPath = $(Join-Path $projectDir -ChildPath $subDir);
    $buildProps = Join-Path -Path $subDirPath -ChildPath "Directory.Build.props"
    UpdateVersionInFile $buildProps $version
}

function UpdateVersionInFile ( $file, $version ) {
    $versionPattern = [regex]'(?sm)<Version>[A-Za-z0-9-\.\r\n\s]*</Version>'
    $inputText = [System.IO.File]::ReadAllText($file)
    $result = $versionPattern.Replace($inputText, "<Version>$version</Version>")
    [System.IO.File]::WriteAllText($file, $result, [System.Text.Encoding]::UTF8)
}

function UpdateVersionInNuspec ( $file, $version ) {
    $versionPattern = [regex]'(?sm)<version>[A-Za-z0-9-\.\r\n\s]*</version>'
    $clientDepPattern = [regex]'(?sm)<dependency id="RavenDB.Client" version="[A-Za-z0-9-\.\r\n\s]*" exclude="Build,Analyzers" />'
    $result = [System.IO.File]::ReadAllText($file)
    $result = $versionPattern.Replace($result, "<version>$version</version>")
    $result = $clientDepPattern.Replace(`
        $result, "<dependency id=""RavenDB.Client"" version=""$version"" exclude=""Build,Analyzers"" />")
    [System.IO.File]::WriteAllText($file, $result, [System.Text.Encoding]::UTF8)
}

function UpdateCommonAssemblyInfo ( $projectDir, $buildNumber, $version, $commit ) {
    $assemblyInfoFile = [io.path]::combine($projectDir, "src", "CommonAssemblyInfo.cs")
    Write-Host "Set version in $assemblyInfoFile..."

    $fileVersion = "$($version.Split("-")[0]).$buildNumber"

    $result = [System.IO.File]::ReadAllText($assemblyInfoFile)

    $assemblyFileVersionPattern = [regex]'\[assembly: AssemblyFileVersion\(".*"\)\]';
    $result = $assemblyFileVersionPattern.Replace($result, "[assembly: AssemblyFileVersion(""$fileVersion"")]"); 

    $assemblyInfoVersionPattern = [regex]'\[assembly: AssemblyInformationalVersion\(".*"\)\]';
    $result = $assemblyInfoVersionPattern.Replace($result, "[assembly: AssemblyInformationalVersion(""$version"")]")

    [System.IO.File]::WriteAllText($assemblyInfoFile, $result, [System.Text.Encoding]::UTF8)
}


function UpdateRavenVersion ( $projectDir, $buildNumber, $version, $commit, $versionInfoFile ) {
    write-host "Set version in $versionInfoFile"

    $releaseDate = $(Get-Date).ToUniversalTime().ToString("yyyy-MM-dd")
    $content = (Get-Content $versionInfoFile) |
        Foreach-Object { 
            $_ -replace `
                'RavenVersion\(Build = ".*", CommitHash = ".*", Version = "5.2", FullVersion = ".*", ReleaseDateString = ".*"\)', `
                "RavenVersion(Build = ""$buildNumber"", CommitHash = ""$commit"", Version = ""5.2"", FullVersion = ""$version"", ReleaseDateString = ""$releaseDate"")" 
            }

    Set-Content -Path $versionInfoFile -Value $content -Encoding UTF8
}

function Get-Git-Commit-Short
{
    $(Get-Git-Commit).Substring(0, 7)
}

function Get-Git-Commit
{
    if (Get-Command "git" -ErrorAction SilentlyContinue) {
        $sha1 = & git rev-parse HEAD
        CheckLastExitCode
        $sha1
    }
    else {
        return "0000000000000000000000000000000000000000"
    }
}
