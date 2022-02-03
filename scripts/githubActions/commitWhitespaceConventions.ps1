$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$pair = "$($env:githubOwner):$($env:GITHUB_TOKEN)"
$encodedCreds = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes($pair))
$basicAuthValue = "Basic $encodedCreds"

$Headers = @{
    Authorization = $basicAuthValue
}

$url = "https://api.github.com/repos/$env:githubOwner/$env:repoName/pulls/$env:ghprNumber/files?page="

$allMatched = $TRUE
$extensions = ".cs", ".ascx", ".xaml", ".cmd", ".ps1", ".coffee", ".config", ".css", ".nuspec", ".scss", ".cshtml", ".htm", ".html", ".js", ".ts", ".msbuild", ".resx", ".ruleset", ".Stylecop", ".targets", ".tt", ".txt", ".vb", ".vbhtml", ".xml", ".xunit", ".java", ".less"

$page = 1
while($TRUE)
{
    $allFiles = Invoke-RestMethod -Method Get -Uri "$url$page" -Headers $Headers
    $page = $page + 1

    if (($allFiles -eq $null) -or ($allFiles.length -eq 0))
    {
        break;
    }

    Foreach ($file in $allFiles) 
    {
        $filename = $file.filename

        if (Test-Path $filename) 
        {
            Write-Host "Processing '$filename'"
            $ext = [System.IO.Path]::GetExtension("$filename")
            if ($extensions -contains $ext)
            {
                $content = Get-Content "$filename"
                $containsTabs = $FALSE

                foreach ($line in $content)
                {
                    if ($containsTabs -eq $TRUE) {
                        break;
                    }

                    $tabIndex = $line.IndexOf([Char] 9)
                    if ($tabIndex -eq -1)
                    {
                        continue;
                    }

                    $chars = $line.ToCharArray()
                    $anyNonTabChars = $FALSE
                    for ($c = 0; $c -lt $chars.Length; $c++)
                    {
                        $char = $chars[$c]
                        $isSpace = $char -eq [Char] 32
                        if ($isSpace -eq $TRUE) {
                            continue;
                        }
                                        
                        $isTab = $char -eq [Char] 9
                        if ($isTab -eq $FALSE)
                        {
                            $anyNonTabChars = $TRUE

                            if ($c -gt $tabIndex)
                            {
                                $containsTabs = $TRUE
                                break;
                            } 
                            else 
                            {
                                break;
                            }
                        }
                    }

                    if ($anyNonTabChars -eq $FALSE)
                    {
                        $containsTabs = $TRUE
                    }
                }
            
                if ($containsTabs -eq $TRUE)
                {
                    $allMatched = $FALSE
                    Write-Host "Detected tabs in '$filename'"
                }
            }
        }
    }
}

if ($allMatched -eq $FALSE)
{
    "false" | Out-File -FilePath status_whitespace.txt -NoNewline
    throw "Files contain tabs"
}
else
{
    "true" | Out-File -FilePath status_whitespace.txt -NoNewline
}
