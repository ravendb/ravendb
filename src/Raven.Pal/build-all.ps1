param (
    [switch]$zeroversion = $false,
    # rebuild at the current pal.ver, e.g. after fixing a toolchain that failed on the previous run
    [switch]$keepversion = $false
)

$ErrorActionPreference = 'Stop'
Push-Location $PSScriptRoot
try {

# The names DynamicNativeLibraryResolver loads and Raven.Server.csproj ships (libs/librvnpal), with what each must be built for.
# A partial set must never be copied: a binary left at the previous version fails RavenDB startup on its platform.
# A toolchain mix-up (vcvars failing under a Developer Command Prompt's cl, a non-x86_64 WSL) still produces a file with the
# expected name, so the architecture is checked too.
$Expected = [ordered]@{
    'librvnpal.linux.x64.so'    = 'ELF x86-64'
    'librvnpal.arm.32.so'       = 'ELF ARM'
    'librvnpal.arm.64.so'       = 'ELF AArch64'
    'librvnpal.mac.x64.dylib'   = 'Mach-O x86_64'
    'librvnpal.mac.arm64.dylib' = 'Mach-O arm64'
    'librvnpal.win.x86.dll'     = 'PE x86'
    'librvnpal.win.x64.dll'     = 'PE x64'
    'librvnpal.win.arm64.dll'   = 'PE arm64'
    'librvnpal.win7.x86.dll'    = 'PE x86'
    'librvnpal.win7.x64.dll'    = 'PE x64'
}

function Get-BinaryArchitecture([string]$Path) {
    # .NET resolves a relative path against the process directory, not the PowerShell location
    $b = [IO.File]::ReadAllBytes((Resolve-Path -LiteralPath $Path).ProviderPath)
    if ($b.Length -ge 20 -and $b[0] -eq 0x7f -and $b[1] -eq 0x45 -and $b[2] -eq 0x4c -and $b[3] -eq 0x46) {
        switch ([BitConverter]::ToUInt16($b, 18)) { 0x3e { return 'ELF x86-64' } 0x28 { return 'ELF ARM' } 0xb7 { return 'ELF AArch64' } }
        return 'ELF (other machine)'
    }
    if ($b.Length -ge 8 -and $b[0] -eq 0xcf -and $b[1] -eq 0xfa -and $b[2] -eq 0xed -and $b[3] -eq 0xfe) {
        switch ([BitConverter]::ToInt32($b, 4)) { 0x01000007 { return 'Mach-O x86_64' } 0x0100000c { return 'Mach-O arm64' } }
        return 'Mach-O (other cpu)'
    }
    if ($b.Length -ge 64 -and $b[0] -eq 0x4d -and $b[1] -eq 0x5a) {
        $pe = [BitConverter]::ToInt32($b, 0x3c)
        if ($pe -gt 0 -and $pe + 6 -le $b.Length) {
            switch ([BitConverter]::ToUInt16($b, $pe + 4)) { 0x14c { return 'PE x86' } 0x8664 { return 'PE x64' } 0xaa64 { return 'PE arm64' } }
        }
        return 'PE (other machine)'
    }
    return 'not an ELF, Mach-O or PE file'
}

# Sparrow.Server compares PAL_VER with rvn_get_pal_ver() at startup. It is updated only once the binaries are in libs\librvnpal,
# so a failed build never leaves PAL_VER ahead of them. Version 0 skips that check, so PAL_VER stays as it is then.
$PalCs = Join-Path $PSScriptRoot '..\Sparrow.Server\Platform\Pal.cs'
$PalVerPattern = 'public const int PAL_VER = \d+;'
if ( $zeroversion -eq $false -and [IO.File]::ReadAllText($PalCs) -notmatch $PalVerPattern ) {
    throw "PAL_VER was not found in $PalCs"
}

$PalVerStr = (Get-Content pal.ver)
[int]$PalVer = [convert]::ToInt32($PalVerStr, 10)

if ( $zeroversion -eq $true ) {
    $PalVer = 0
}
elseif ( $keepversion -eq $false ) {
    $PalVer++
    Set-Content pal.ver -Value $PalVer
}

$GenCode = "#include <sys/types.h>`n#include ""rvn.h""`nEXPORT int32_t rvn_get_pal_ver() { return  $($PalVer) ; }"
Set-Content src/rvngetpalver.c -Value $GenCode

if (Test-Path artifacts) {
    Remove-Item -Recurse .\artifacts\
}

$Failed = @()

if (-not (Test-Path osxcross)) {
    wsl bash -c './build-all-posix.sh setup'
    if ($LASTEXITCODE -ne 0) { $Failed += "build-all-posix.sh setup (exit code $LASTEXITCODE)" }
}

wsl bash -c './build-all-posix.sh build'
if ($LASTEXITCODE -ne 0) { $Failed += "build-all-posix.sh build (exit code $LASTEXITCODE)" }

# .\ so it also runs where NoDefaultCurrentDirectoryInExePath is set
cmd.exe /c .\build-all-windows.bat
if ($LASTEXITCODE -ne 0) { $Failed += "build-all-windows.bat (exit code $LASTEXITCODE)" }

$Missing = @($Expected.Keys | Where-Object { -not (Test-Path (Join-Path artifacts $_)) })
$Unexpected = @(Get-ChildItem artifacts -File -ErrorAction SilentlyContinue | Where-Object { -not $Expected.Contains($_.Name) } | ForEach-Object Name)
$WrongArchitecture = @(foreach ($Name in $Expected.Keys) {
    $Path = Join-Path artifacts $Name
    if (Test-Path $Path) {
        $Actual = Get-BinaryArchitecture $Path
        if ($Actual -ne $Expected[$Name]) { "$Name is $Actual, expected $($Expected[$Name])" }
    }
})

if ($Failed.Count -gt 0 -or $Missing.Count -gt 0 -or $Unexpected.Count -gt 0 -or $WrongArchitecture.Count -gt 0) {
    if ($Failed.Count -gt 0) { Write-Host "Failed: $($Failed -join '; ')" }
    if ($Missing.Count -gt 0) { Write-Host "Not built: $($Missing -join ', ')" }
    if ($Unexpected.Count -gt 0) { Write-Host "Unexpected names (nothing loads them): $($Unexpected -join ', ')" }
    if ($WrongArchitecture.Count -gt 0) { Write-Host "Wrong architecture: $($WrongArchitecture -join '; ')" }
    throw "PAL $PalVer was not built for every platform: nothing was copied to libs\librvnpal and PAL_VER in Pal.cs is unchanged. Fix the build and rerun with -keepversion."
}

Copy-Item ($Expected.Keys | ForEach-Object { Join-Path artifacts $_ }) ..\..\libs\librvnpal\

if ( $zeroversion -eq $false ) {
    $Text = [IO.File]::ReadAllText($PalCs)
    [IO.File]::WriteAllText($PalCs, [regex]::Replace($Text, $PalVerPattern, "public const int PAL_VER = $PalVer;"))
}

Write-Host "PAL $PalVer built for all $($Expected.Count) platforms, copied to libs\librvnpal and set as PAL_VER in Pal.cs."
Write-Host "Commit libs\librvnpal, pal.ver, src\rvngetpalver.c and src\Sparrow.Server\Platform\Pal.cs together."
}
finally {
    Pop-Location
}
