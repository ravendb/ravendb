$ErrorActionPreference = "Stop"

$url = $env:CLA_SIGNED_URL + $env:ghprbPullId
Invoke-RestMethod -Method Get -Uri $url -ContentType "application/json" -UseBasicParsing