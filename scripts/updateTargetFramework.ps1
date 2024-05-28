param($Version)

function UpdateCsprojTargetFramework ( $csproj, $version ) {
    $versionPattern = [regex]'(?sm)<TargetFramework>[A-Za-z0-9-\.\r\n\s]*</TargetFramework>'
    $result = [System.IO.File]::ReadAllText($csproj)
    $result = $versionPattern.Replace($result, "<TargetFramework>$version</TargetFramework>")
    [System.IO.File]::WriteAllText($csproj, $result, [System.Text.Encoding]::UTF8)
}

if ([string]::IsNullOrEmpty($Version)) {
    throw "Version is required."
}

$csprojs = Get-ChildItem -Recurse "*.csproj"

foreach ($csproj in $csprojs) {
    write-host "Update TargetFramework in $csproj"
    UpdateCsprojTargetFramework $csproj $Version
}

