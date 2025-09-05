param(
    [Parameter(Mandatory = $true)]
    [string] $owner,
    [Parameter(Mandatory = $true)]
    [string] $repo,
    [Parameter(Mandatory = $true)]
    [string] $pullRequestId,
    [Parameter(Mandatory = $true)]
    [string[]] $labels
)

$uri = "https://api.github.com/repos/$owner/$repo/issues/$pullRequestId/labels"

$body = @{ labels = $labels } | ConvertTo-Json -Compress

Write-Host "Request body: $body"

Invoke-RestMethod -Method Post -Uri $uri -Verbose -Headers @{
  Authorization = "Bearer $env:GITHUB_TOKEN"
  Accept        = "application/vnd.github+json"
  "User-Agent"  = "$owner-$repo-labeler"
  "X-GitHub-Api-Version" = "2022-11-28"
} -Body $body -ContentType 'application/json'
