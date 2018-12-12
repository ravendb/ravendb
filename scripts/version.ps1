function SetTeamCityEnvironmentVariable ( $name, $value ) {
    Write-Host "##teamcity[setParameter name='$name' value='$value']"
}

function SetVersionEnvironmentVariableInTeamCity($version) {
    SetTeamCityEnvironmentVariable 'env.informationalVersion' $version
}

function SetBuiltAtEnvironmentVariableInTeamCity($builtAt) {
    SetTeamCityEnvironmentVariable 'env.BUILT_AT' $($builtAt.ToString('o'))
}

$DEV_BUILD_NUMBER = 42
function GetBuildNumber () {
    if ($env:BUILD_NUMBER) {
        $result = $env:BUILD_NUMBER
    }
    else {
        $result = $DEV_BUILD_NUMBER
    }

    $result
}

function GetBuildType () {
    if ($env:BUILD_TYPE) {
        $result = $env:BUILD_TYPE
    }
    else {
        $result = "custom";
    }

    $result
}

$RELEASE_INFO_FILE = 'artifacts/release-info.json'
function SetVersionInfo($projectDir) {
    $buildNumber = GetBuildNumber
    $buildType = GetBuildType
    $builtAt = [DateTime]::UtcNow
    $builtAtString = $builtAt.ToString("yyyyMMdd-HHmm")

    if ($buildType.ToLower() -eq 'nightly') {
        $versionSuffix = "$buildType-$builtAtString"
        $buildNumber = $DEV_BUILD_NUMBER
    }
    else {
        $versionSuffix = "$buildType-$buildNumber"
    }

    $versionPrefix = GetCurrentVersionPrefix $projectDir

    $version = $versionPrefix
    if ($buildType.ToLower() -ne 'stable') {
        $version = "$versionPrefix-$versionSuffix"
    }

    SetVersionEnvironmentVariableInTeamCity $version
    SetBuiltAtEnvironmentVariableInTeamCity $builtAt
    
    $versionInfo = @{ 
        Version       = $version;
        VersionPrefix = $versionPrefix;
        VersionSuffix = $versionSuffix;
        BuildNumber   = $buildNumber;
        BuiltAt       = $builtAt;
        BuiltAtString = $builtAtString;
        BuildType     = $buildType;
    }

    New-Item -Path $RELEASE_INFO_FILE -Force -Type File
    $versionInfoJson = ConvertTo-Json -InputObject $versionInfo
    Set-Content -Path $RELEASE_INFO_FILE -Value $versionInfoJson

    return $versionInfo
}

function GetVersionInfo() {
    return Get-Content -Path $RELEASE_INFO_FILE | ConvertFrom-Json
}

function BumpVersion ($projectDir, $versionPrefix, $buildType, $dryRun = $False) {
    if ($buildType.ToLower() -ne "stable") {
        return
    }

    write-host "Calculate new version"
    $newVersion = SemverMinor $versionPrefix
    write-host "New version is: $newVersion"

    $repo = @{
        "Owner"  = "ravendb"
        "Name"   = "ravendb"
        "Branch" = "v4.2"
    }

    $remoteFilePath = 'src/CommonAssemblyInfo.cs'
    $fileUri = GetGitHubFileUri $repo $remoteFilePath
    $githubFileData = GetFileDataFromGitHub $fileUri $repo.Branch
    $contents = GetAssemblyInfoWithBumpedVersion $projectDir $newVersion $githubFileData.content
    BumpVersionInRemoteRepo $fileUri $contents $repo $githubFileData $newVersion $dryRun

    $remoteFilePath = 'src/Raven.Client/Properties/VersionInfo.cs'
    $fileUri = GetGitHubFileUri $repo $remoteFilePath
    $githubFileData = GetFileDataFromGitHub $fileUri $repo.Branch
    $contents = GetVersionInfoWithBumpedVersion $projectDir $newVersion $githubFileData.content
    BumpVersionInRemoteRepo $fileUri $contents $repo $githubFileData $newVersion $dryRun
}

function BumpVersionInRemoteRepo($fileUri, $contents, $repo, $githubFileData, $newVersion, $dryRun) {
    $commitMessage = "Bump version in $remoteFilePath to $newVersion"
    if ($dryRun) {
        write-host "DRY RUN: Bumped version in the repository $($repo.Owner)/$($repo.Name) $($repo.Branch) to $newVersion."
        write-host "DRY RUN: Updated contents for $($fileUri): "
        write-host $contents
        write-host "DRY RUN: Commit msg: $commitMessage"

        return
    }

    UpdateFileInGitHub $fileUri $contents $commitMessage $repo.Branch $githubFileData
    write-host "Bumped version in the repository $($repo.Owner)/$($repo.Name) $($repo.Branch) to $newVersion."
}

function GetGithubFileUri($repoInfo, $filePath) {

    $repoOwner = $repoInfo.Owner
    $repoName = $repoInfo.Name

    write-host "Build file URI for: $repoOwner/$($repoName):$($filePath)"

    if (!$repoOwner) {
        throw "Repository owner is required."
    }

    if (!$repoName) {
        throw "Repository name is required."
    }

    if (!$filePath) {
        throw "File path to update is required."
    }

    return "https://api.github.com/repos/$repoOwner/$repoName/contents/$filepath"
}

function UpdateFileInGitHub($fileUri, $newFileContent, $commitMessage, $branch, $githubFileInfo) {

    if (!$githubFileInfo) {
        throw "Github file info is required."
    }

    if (!$commitMessage) {
        throw "Commit message is required."
    }

    if (!$env:GITHUB_USER -or !$env:GITHUB_ACCESS_TOKEN) {
        throw "Environment variables holding GitHub credentials GITHUB_USER or GITHUB_ACCESS_TOKEN are not set."
    }

    if (!$branch) {
        $branch = exec { & "git" rev-parse --abbrev-ref HEAD }
    }
    
    $bodyJson = ConvertTo-Json @{
        path    = $githubFileInfo.path
        sha     = $githubFileInfo.sha
        branch  = $branch
        message = $commitMessage
        content = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($newFileContent))
    }

    $creds = "{0}:{1}" -f $env:GITHUB_USER, $env:GITHUB_ACCESS_TOKEN
    $encodedCreds = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($creds))
    $headers = @{
        Authorization = "Basic $encodedCreds"
    }
    
    write-host "Update $fileUri file in GIT repo." 

    (Invoke-WebRequest -TimeoutSec 120 -Uri $fileUri -Method PUT -Headers $headers -ContentType "application/json" -Body $bodyJson).content | ConvertFrom-Json
    
    write-host "Updated content under $fileUri"
}

function GetAssemblyInfoContent($fileUri, $branch) {
    write-host "Getting $fileUri`?ref=$branch";
    $fileContent = (Invoke-RestMethod -TimeoutSec 120 "$fileUri`?ref=$branch").content
    [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($fileContent))
}

function GetFileDataFromGitHub($fileUri, $branch) {
    if (!$branch) {
        throw "Branch is mandatory."
    }

    $data = Invoke-RestMethod -TimeoutSec 120 "$fileUri`?ref=$branch"
    $data
}

function GetAssemblyInfoWithBumpedVersion ($projectDir, $newVersion, $srcFileContent) {
    Write-Host "Set version in CommonAssemblyInfo.cs ..."

    $result = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($srcFileContent))

    $assemblyVersionPattern = [regex]'\[assembly: AssemblyVersion\(".*"\)\]'
    $result = $assemblyVersionPattern.Replace($result, "[assembly: AssemblyVersion(""$newVersion"")]")

    $assemblyFileVersionPattern = [regex]'\[assembly: AssemblyFileVersion\(".*"\)\]'
    $result = $assemblyFileVersionPattern.Replace($result, "[assembly: AssemblyFileVersion(""$newVersion.42"")]")

    $assemblyInfoVersionPattern = [regex]'\[assembly: AssemblyInformationalVersion\(".*"\)\]';
    $result = $assemblyInfoVersionPattern.Replace($result, "[assembly: AssemblyInformationalVersion(""$newVersion"")]")

    if (!$result) {
        throw "Could not get assembly info file contents with bumped version."
    }

    return $result
}

function GetVersionInfoWithBumpedVersion ($projectDir, $newVersion, $srcFileContent) {
    Write-Host "Set version in VersionInfo.cs ..."

    $result = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($srcFileContent))

    $pattern = [regex]'\[assembly: RavenVersion\(Build = "42", CommitHash = "([^"]*)", Version = "4.2", FullVersion = "[^"]*"\)\]'
    $m = $pattern.Match($result)
    $commit = $m.Groups[1]
    $result = $pattern.Replace(
        $result,
        "[assembly: RavenVersion(Build = ""42"", CommitHash = ""$commit"", Version = ""4.2"", FullVersion = ""$newVersion-custom-42"")]")

    if (!$result) {
        throw "Could not get VersionInfo.cs file contents with bumped version."
    }

    return $result
}

function SemverMinor ($versionPrefix) {
    $versionStrings = $versionPrefix.split('.')
    $versionNumbers = foreach ($number in $versionStrings) {
        [int]::parse($number)
    }

    $versionNumbers[2] += 1
    $newVersion = $versionNumbers -join '.'
    
    $newVersion
}

function GetCurrentVersionPrefix($projectDir) {
    $commonAssemblyInfoFile = [io.path]::combine($projectDir, 'src', 'CommonAssemblyInfo.cs')
    $match = select-string -Path $commonAssemblyInfoFile -Pattern 'AssemblyVersion\("(.*)"\)'
    $match.Matches.Groups[1].Value
}

function Validate-AssemblyVersion($assemblyPath, $versionInfo) {
    # $versionInfo = @{ 
    #     Version = $version;
    #     VersionPrefix = $versionPrefix;
    #     VersionSuffix = $versionSuffix;
    #     BuildNumber = $buildNumber;
    #     BuiltAt = $builtAt;
    #     BuiltAtString = $builtAtString;
    #     BuildType = $buildType;
    # }
    Assert-AssemblyVersion `
        -ExpectedVersion $versionInfo.VersionPrefix `
        -AssemblyPath $assemblyPath
    
    Assert-AssemblyFileVersion `
        -ExpectedFileVersion "$($versionInfo.VersionPrefix).$($versionInfo.BuildNumber)" `
        -AssemblyPath $assemblyPath

    Assert-AssemblyProductVersion `
        -ExpectedProductVersion $versionInfo.Version `
        -AssemblyPath $assemblyPath

    write-host "Version valid for $assemblyPath"

}
