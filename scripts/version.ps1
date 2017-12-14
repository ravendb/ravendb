function SetTeamCityEnvironmentVariable ( $name, $value ) {
    Write-Host "##teamcity[setParameter name='$name' value='$value']"
}

function SetVersionEnvironmentVariableInTeamCity($version) {
    SetTeamCityEnvironmentVariable 'env.informationalVersion' $version
}

function SetBuiltAtEnvironmentVariableInTeamCity($builtAt) {
    SetTeamCityEnvironmentVariable 'env.BUILT_AT' $($builtAt.ToString('o'))
}

$DEV_BUILD_NUMBER = 40
function GetBuildNumber () {
    if ($env:BUILD_NUMBER) {
        $result = $env:BUILD_NUMBER
    } else {
        $result = $DEV_BUILD_NUMBER
    }

    $result
}

function GetBuildType () {
    if ($env:BUILD_TYPE) {
        $result = $env:BUILD_TYPE
    } else {
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
    } else {
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
        Version = $version;
        VersionPrefix = $versionPrefix;
        VersionSuffix = $versionSuffix;
        BuildNumber = $buildNumber;
        BuiltAt = $builtAt;
        BuiltAtString = $builtAtString;
        BuildType = $buildType;
    }

    New-Item -Path $RELEASE_INFO_FILE -Force -Type File
    $versionInfoJson = ConvertTo-Json -InputObject $versionInfo
    Set-Content -Path $RELEASE_INFO_FILE -Value $versionInfoJson

    return $versionInfo
}

function GetVersionInfo() {
    return Get-Content -Path $RELEASE_INFO_FILE | ConvertFrom-Json
}

function BumpVersion ($projectDir, $versionPrefix, $buildType) {
    if ($buildType.ToLower() -ne "stable") {
        return
    }

    $repoOwner = "ravendb"
    $repo = "ravendb"
    $branch = "v4.0"
    $remoteFilePath = 'src/CommonAssemblyInfo.cs'
    
    write-host "Build file URI for: $repoOwner/$($repo):$($remoteFilePath)"
    $fileUri = GetGitHubFileUri $repoOwner $repo $remoteFilePath
    $githubFileData = GetFileDataFromGitHub $fileUri $branch
    $origFileContent = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($githubFileData.content))
    
    write-host "Calculate new version"
    $newVersion = SemverMinor $version
    write-host "New version is: $newVersion"
    
    write-host "Get updated file contents for $fileUri"
    $assemblyInfoFileContent = GetAssemblyInfoWithBumpedVersion $origFileContent $newVersion

    if (!$assemblyInfoFileContent) {
        return
    }
    
    $commitMessage = "Bump version to $newVersion"
    
    UpdateFileInGitHub $fileUri $assemblyInfoFileContent $commitMessage $branch $githubFileData

    write-host "Bumped version in the repository $repoOwner/$repo ($branch) to $newVersion."
}


function GetGithubFileUri($repoOwner, $repoName, $filePath) {
    if (!$repoOwner) {
        throw "Repository owner is required."
    }

    if (!$repoName) {
        throw "Repository name is required."
    }

    if (!$filePath) {
        throw "File path to update is required."
    }

    $fileUri = "https://api.github.com/repos/$repoOwner/$repoName/contents/$filepath"
    
    $fileUri
}

function UpdateFileInGitHub($fileUri, $newFileContent, $commitMessage, $branch, $githubFileInfo) {

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
        path = $githubFileInfo.path
        sha = $githubFileInfo.sha
        branch = $branch
        message = $commitMessage
        content = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($newFileContent))
    }

    $creds = "{0}:{1}" -f $env:GITHUB_USER,$env:GITHUB_ACCESS_TOKEN
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
    $data = Invoke-RestMethod -TimeoutSec 120 "$fileUri`?ref=$branch"
    $data
}

function GetAssemblyInfoWithBumpedVersion ($origFileContent, $newVersion) {
    $assemblyInfoFileContents = $origFileContent |
    Foreach-Object { $_ -replace '\[assembly: AssemblyVersion\(".*"\)\]', "[assembly: AssemblyVersion(""$newVersion"")]" } |
    Foreach-Object { $_ -replace '\[assembly: AssemblyFileVersion\(".*"\)\]', "[assembly: AssemblyFileVersion(""$newVersion.40"")]" } |
    Foreach-Object { $_ -replace '\[assembly: AssemblyInformationalVersion\(".*"\)\]', "[assembly: AssemblyInformationalVersion(""$newVersion"")]" } |
    Out-String

    "{0}`r`n" -f $assemblyInfoFileContents.TrimEnd()
}

function SemverMinor ($versionPrefix) {
    $versionStrings = $versionPrefix.split('.')
    $versionNumbers = foreach($number in $versionStrings) {
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
