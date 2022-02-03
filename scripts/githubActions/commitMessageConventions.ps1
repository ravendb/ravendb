$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$pair = "$($env:githubOwner):$($env:GITHUB_TOKEN)"
$encodedCreds = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($pair))
$basicAuthValue = "Basic $encodedCreds"

$Headers = @{
    Authorization = $basicAuthValue
}

$url = "https://api.github.com/repos/$env:githubOwner/$env:repoName/pulls/$env:ghprNumber/commits"

echo "$url"

$response = Invoke-WebRequest -Method 'GET' -Uri $url -Headers $Headers -UseBasicParsing
$rateLimit = $response.Headers["X-RateLimit-Limit"]
$rateLimitRemaining = $response.Headers["X-RateLimit-Remaining"]
Write-Host "GitHub API rate limit remaining: $rateLimitRemaining (out of $rateLimit)"

$allCommits = $($response) | ConvertFrom-Json
$allMatched = $TRUE

echo "$allCommits"

Foreach ($commit in $allCommits)
{
    $message = $commit.commit.message
    Write-Host "Processing message '$message'"

    $loweredMessage = $message.ToLowerInvariant()
    $match = $loweredMessage -match "ravendb-\d+" -or $loweredMessage -match "rdoc-\d+" -or $loweredMessage -match "rdbqa-\d+" -or $loweredMessage -match "rdbc-\d+" -or $loweredMessage -match "rdbs-\d+" -or $loweredMessage -match "rdbcl-\d+" -or $loweredMessage -match "merge branch" -or $loweredMessage -match "merge remote" -or $loweredMessage -match "merge pull request"

    if ($match -eq $FALSE)
    {
        $allMatched = $FALSE
        Write-Host "Commit message '$message' does not contain issue #"
    }
}

if ($allMatched -eq $FALSE)
{
    "false" | Out-File -FilePath status_message.txt -NoNewline
    throw "Not all commit messages contain issue #"
}
else
{
    "true" | Out-File -FilePath status_message.txt -NoNewline
}
