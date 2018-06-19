param($Version)

function UpdateCsprojTargetFrameworkVersion ( $csproj, $version ) {
    $versionPattern = [regex]'(?sm)<RuntimeFrameworkVersion>[A-Za-z0-9-\.\r\n\s]*</RuntimeFrameworkVersion>'
    $result = [System.IO.File]::ReadAllText($csproj)
    $result = $versionPattern.Replace($result, "<RuntimeFrameworkVersion>$version</RuntimeFrameworkVersion>")
    [System.IO.File]::WriteAllText($csproj, $result, [System.Text.Encoding]::UTF8)
}

function UpdateServerOptions ( $serverOptionsFile, $version ) {
    $versionPattern = [regex]'(?sm)public string FrameworkVersion { get; set; } = "[A-Za-z0-9-\.\r\n\s]*";'
    $result = [System.IO.File]::ReadAllText($serverOptionsFile)
    $result = $versionPattern.Replace($result, "public string FrameworkVersion { get; set; } = ""$version"";")
    [System.IO.File]::WriteAllText($serverOptionsFile, $result, [System.Text.Encoding]::UTF8)
}

if ([string]::IsNullOrEmpty($Version)) {
    throw "Version is required."
}

$csprojs = Get-ChildItem -Recurse "*.csproj"

foreach ($csproj in $csprojs) {
    write-host "Update TargetFrameworkVersion in $csproj"
    UpdateCsprojTargetFrameworkVersion $csproj $Version
}

write-host "Update FrameworkVersion in ServerOptions.cs"
UpdateServerOptions $(Get-ChildItem ".\src\Raven.Embedded\ServerOptions.cs").FullName $Version

