param(
    [string] $owner,
    [string] $repo,
    [string] $pullRequestId,
    [string] $label
)

$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::Expect100Continue = $true;
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12;

$IGNORED_LABEL = "future";

function Invoke-JsonHttpPost($url, $text) {
    Write-Host "[INFO] POSTing to $url"

    $webRequest = [System.Net.WebRequest]::Create($url)
    $webRequest.ContentType = "application/json"
    $PostStr = [System.Text.Encoding]::Default.GetBytes($text)
    $webrequest.ContentLength = $PostStr.Length
    $webRequest.Headers["Authorization"] = "token $env:GITHUB_TOKEN"
    $webRequest.UserAgent = "custom";
    $webRequest.PreAuthenticate = $true
    $webRequest.Method = "POST"
    
    $requestStream = $webRequest.GetRequestStream()
    $requestStream.Write($PostStr, 0, $PostStr.length)
    $requestStream.Close()
    
    $resp = $webRequest.GetResponse();
    $rs = $resp.GetResponseStream();
    [System.IO.StreamReader] $sr = New-Object System.IO.StreamReader -argumentList $rs;
    [string] $results = $sr.ReadToEnd();
    
    Write-Host "[INFO] response:"
    Write-Host $results;
    return;
}

function Invoke-JsonHttpGet($url) {
    Write-Host "[INFO] getting $url"

    $webRequest = [System.Net.WebRequest]::Create($url)
    $webRequest.ContentType = "application/json"
    $PostStr = [System.Text.Encoding]::Default.GetBytes("")
    $webrequest.ContentLength = $PostStr.Length
    $webRequest.Headers["Authorization"] = "token $env:GITHUB_TOKEN"
    $webRequest.UserAgent = "custom";
    $webRequest.PreAuthenticate = $true
    $webRequest.Method = "GET"
    
    $resp = $webRequest.GetResponse();
    $rs = $resp.GetResponseStream();
    [System.IO.StreamReader] $sr = New-Object System.IO.StreamReader -argumentList $rs;
    [string] $results = $sr.ReadToEnd();
    
    Write-Host "[INFO] response:"
    Write-Host $results;
    return $results | ConvertFrom-Json;
}

function Get-ExistingLabels() {
    $response = Invoke-JsonHttpGet "https://api.github.com/repos/$owner/$repo/issues/$pullRequestId/labels"
    return $response | Select-Object name | ForEach-Object {$_.name}
}


$labels = Get-ExistingLabels

if ($labels -Contains $label) {
    Write-Host "[INFO] Pull request #$pullRequestId already contains label `"$label`""
} elseif ($labels -Contains $IGNORED_LABEL) {
    Write-Host "[INFO] Encountered label `"$IGNORED_LABEL`". Skipping `"$label`" label assignment for pull request #$pullRequestId"
} else {
    Write-Host "[INFO] Adding label `"$label`" to pull request #$pullRequestId"
    Invoke-JsonHttpPost "https://api.github.com/repos/$owner/$repo/issues/$pullRequestId/labels" "[`"$label`"]"
}

