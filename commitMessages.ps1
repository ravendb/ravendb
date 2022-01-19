$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$pair = "$($env:githubOrganization):$($env:GITHUB_TOKEN)"
$encodedCreds = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($pair))
$basicAuthValue = "Basic $encodedCreds"

$Headers = @{
	Authorization = $basicAuthValue
}

$url = "https://api.github.com/repos/$env:githubOrganization/$env:repoName/pulls/$env:ghprbPullId/commits"

$env:response = Invoke-WebRequest -Method 'GET' -Uri $url -Headers $Headers -UseBasicParsing
$rateLimit = $env:response.Headers["X-RateLimit-Limit"]
$rateLimitRemaining = $env:response.Headers["X-RateLimit-Remaining"]
Write-Host "GitHub API rate limit remaining: $rateLimitRemaining (out of $rateLimit)"

echo "::set-env name=commits::$env:response}"
