# Packs the libzstd binaries built by build-zstd.ps1 (build-libs/zstd.sh in Docker, MSVC for win-arm64, osxcross or
# build-libs/build-zstd-macos.sh for macOS) as the RavenDB.Zstd.Native package, runtimes/<rid>/native/libzstd.*, puts it in libs/
# and updates Directory.Packages.props. RavenDB.Client depends on it, so every new version has to be published to nuget.org.
#
# -BinariesPath  folder with the libzstd.<platform> files (default: artifacts)
# -ZstdVersion   zstd release the binaries were built from (LIBZSTD_VER in build-libs/zstd-build-deps.sh), checked in every binary
# -Revision      bumps the 4th part of the package version when the same zstd version is packed again
param ( [string]$BinariesPath = (Join-Path $PSScriptRoot "artifacts"), [string]$ZstdVersion = "v1.5.7", [int]$Revision = 0 )

$ErrorActionPreference = 'Stop'

$BinariesPath = (Resolve-Path $BinariesPath).Path
$version = $ZstdVersion.TrimStart('v')
if ($version -notmatch '^\d+\.\d+\.\d+$') {
    throw "Cannot make a package version out of '$ZstdVersion', expected a release tag like v1.5.7."
}
$packageVersion = if ($Revision -gt 0) { "$version.$Revision" } else { $version }

# runtime -> names the build scripts give the binary (libs/libzstd used the second spelling for ARM); macOS x64 isn't shipped
$layout = [ordered]@{
    "win-x86"     = @("libzstd.win.x86.dll")
    "win-x64"     = @("libzstd.win.x64.dll")
    "win-arm64"   = @("libzstd.win.arm64.dll")
    "linux-x64"   = @("libzstd.linux.x64.so")
    "linux-arm"   = @("libzstd.arm.32.so", "libzstd.linux.arm.32.so")
    "linux-arm64" = @("libzstd.arm.64.so", "libzstd.linux.arm.64.so")
    "osx-arm64"   = @("libzstd.mac.arm64.dylib")
}

$packagePath = Join-Path $BinariesPath "RavenDB.Zstd.Native"
Remove-Item $packagePath -Force -Recurse -ErrorAction Ignore
New-Item -ItemType Directory -Path $packagePath -Force > $null

foreach ($rid in $layout.Keys) {
    $source = $layout[$rid] | ForEach-Object { Join-Path $BinariesPath $_ } | Where-Object { Test-Path $_ } | Select-Object -First 1
    if ($null -eq $source) {
        throw "Missing libzstd for $rid in $BinariesPath (expected one of: $($layout[$rid] -join ', '))."
    }

    # ZSTD_versionString() is a literal in every build, so a binary of another zstd version can't slip into the package
    $text = [Text.Encoding]::ASCII.GetString([IO.File]::ReadAllBytes($source))
    if ($text.Contains("`0$version`0") -eq $false) {
        throw "$source is not zstd $version."
    }

    $outDir = Join-Path $packagePath "runtimes\$rid\native"
    New-Item -ItemType Directory -Path $outDir -Force > $null
    Copy-Item $source (Join-Path $outDir ("libzstd" + [IO.Path]::GetExtension($source)))
    Write-Output "$rid <- $(Split-Path $source -Leaf)"
}

Copy-Item (Join-Path $PSScriptRoot "zstd.LICENSE.txt") (Join-Path $packagePath "LICENSE.txt")
Copy-Item (Join-Path $PSScriptRoot "zstd.README.md") (Join-Path $packagePath "README.md")
Copy-Item (Join-Path $PSScriptRoot "..\..\scripts\assets\icon.png") (Join-Path $packagePath "icon.png")

$nuspecPath = Join-Path $packagePath "zstd.nuspec"
$nuspec = (Get-Content (Join-Path $PSScriptRoot "zstd.nuspec.template") -Raw).Replace("NUGET_PACKAGE_VERSION", $packageVersion).Replace("ZSTD_VERSION", $ZstdVersion)
Set-Content $nuspecPath -Value $nuspec

# dummy project, required to have dotnet pack working; kept apart so its obj folder does not end up in the package
$projectPath = Join-Path $BinariesPath "RavenDB.Zstd.Native.pack"
Remove-Item $projectPath -Force -Recurse -ErrorAction Ignore
New-Item -ItemType Directory -Path $projectPath -Force > $null
Set-Content (Join-Path $projectPath "project.csproj") -Value "<Project Sdk='Microsoft.NET.Sdk'><PropertyGroup><TargetFramework>netstandard2.0</TargetFramework></PropertyGroup></Project>"

dotnet pack (Join-Path $projectPath "project.csproj") "-p:NuspecFile=$nuspecPath" "-p:NuspecBasePath=$packagePath" -p:NoBuild=true --output $packagePath
if ($LASTEXITCODE -ne 0) {
    throw "Failed to pack RavenDB.Zstd.Native $packageVersion."
}
Remove-Item $projectPath -Force -Recurse

$libsPath = Resolve-Path (Join-Path $PSScriptRoot "..\..\libs")
Remove-Item (Join-Path $libsPath "RavenDB.Zstd.Native.*.nupkg") -ErrorAction Ignore
Copy-Item (Join-Path $packagePath "RavenDB.Zstd.Native.$packageVersion.nupkg") $libsPath

# text replace instead of an XML round trip, so the rest of the file keeps its formatting
$propsPath = Resolve-Path (Join-Path $PSScriptRoot "..\..\Directory.Packages.props")
$propsBytes = [IO.File]::ReadAllBytes($propsPath)
$propsHasBom = $propsBytes.Length -ge 3 -and $propsBytes[0] -eq 0xEF -and $propsBytes[1] -eq 0xBB -and $propsBytes[2] -eq 0xBF
$props = [IO.File]::ReadAllText($propsPath)
if ($props -match '<PackageVersion Include="RavenDB\.Zstd\.Native" Version="[^"]*" />') {
    $props = $props -replace '<PackageVersion Include="RavenDB\.Zstd\.Native" Version="[^"]*" />', "<PackageVersion Include=""RavenDB.Zstd.Native"" Version=""$packageVersion"" />"
} else {
    $props = $props -replace '(?m)^(\s*)(<PackageVersion Include="Raven\.Pal" Version="[^"]*" />)(\r?\n)', "`$1`$2`$3`$1<PackageVersion Include=""RavenDB.Zstd.Native"" Version=""$packageVersion"" />`$3"
}
[IO.File]::WriteAllText($propsPath, $props, (New-Object System.Text.UTF8Encoding($propsHasBom)))

Write-Output "Packed RavenDB.Zstd.Native $packageVersion into $libsPath"
