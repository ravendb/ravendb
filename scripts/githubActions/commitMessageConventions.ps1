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

# Attribution patterns that should not appear in commit messages
$attributionPattern = '(?i)^\s*(co-authored-by|signed-off-by|reviewed-by|helped-by|generated-by|assisted-by)\s*:'

Foreach ($commit in $allCommits)
{
    $message = $commit.commit.message
    Write-Host "Processing message '$message'"

    $loweredMessage = $message.ToLowerInvariant()

    # Rule 1: Title must contain a recognized issue key or be a merge/bump/WIP commit
    $match = $loweredMessage -match "^(ravendb|rdoc|rdbqa|rdbc|rdbs|rdbcl|hrint)-\d+" `
        -or $loweredMessage -match "^merge branch" `
        -or $loweredMessage -match "^merge remote" `
        -or $loweredMessage -match "^merge pull request" `
        -or $loweredMessage -match "^bump version" `
        -or $loweredMessage -match "^wip($|\s)"

    if ($match -eq $FALSE)
    {
        $allMatched = $FALSE
        Write-Host "Commit message '$message' does not match allowed title format"
    }

    # Rule 2: Title with issue key must have a description after the separator
    if ($loweredMessage -match "^(ravendb|rdoc|rdbqa|rdbc|rdbs|rdbcl|hrint)-\d+$")
    {
        $allMatched = $FALSE
        Write-Host "Commit message '$message' has an issue key but no description"
    }

    # Rule 3: No attribution lines in the body
    $lines = $message -split "`n"
    foreach ($line in $lines)
    {
        if ($line -match $attributionPattern)
        {
            $allMatched = $FALSE
            Write-Host "Commit message contains attribution line: '$line'"
            break
        }
    }
}

if ($allMatched -eq $FALSE)
{
    "false" | Out-File -FilePath status_message.txt -NoNewline
    throw "Not all commit messages meet conventions"
}
else
{
    "true" | Out-File -FilePath status_message.txt -NoNewline
}
