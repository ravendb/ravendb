
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

    $creds = "{0}:{1}" -f $global:githubUser,$global:githubAccessToken
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

function GetAssemblyInfoWithBumpedVersion ($origFileContent, $newVersion, $newInformationalVersion) {
    $assemblyInfoFileContents = $origFileContent |
    Foreach-Object { $_ -replace '\[assembly: AssemblyVersion\(".*"\)\]', "[assembly: AssemblyVersion(""$newVersion"")]" } |
    Foreach-Object { $_ -replace '\[assembly: AssemblyFileVersion\(".*"\)\]', "[assembly: AssemblyFileVersion(""$newVersion.13"")]" } |
    Foreach-Object { $_ -replace '\[assembly: AssemblyInformationalVersion\(".*"\)\]', "[assembly: AssemblyInformationalVersion(""$newInformationalVersion"")]" } |
    Out-String

    "{0}`r`n" -f $assemblyInfoFileContents.TrimEnd()
}

function CalcNewVersion ($version) {
    $versionStrings = $version.split('.')
    $versionNumbers = foreach($number in $versionStrings) {
            [int]::parse($number)
    }

    $versionNumbers[2] += 1
    $newVersion = $versionNumbers -join '.'
    
    $newVersion
}
