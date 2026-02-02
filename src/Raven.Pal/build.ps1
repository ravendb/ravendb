param ( [switch]$skip_version_increment = $false )


if ($null -eq (Get-Command "zig" -ErrorAction SilentlyContinue)) {
    Write-Output "Missing zig installation! Execute this command to install zig:"
    Write-Output "*******************************************************"
    Write-Output "  winget install -e --id zig.zig"
    Write-Output "*******************************************************"
    exit 1
}

$PalVerStr = (Get-Content pal.ver)
[int]$PalVer = [convert]::ToInt32($PalVerStr, 10)


if ( $skip_version_increment -eq $false ) { 
    $PalVer++
    $PalVerStr = $PalVer.ToString()
    Set-Content pal.ver -Value $PalVer 
}

$GenCode = "#include <sys/types.h>`n#include ""rvn.h""`nEXPORT int32_t rvn_get_pal_ver() { return  $($PalVer) ; }"
Set-Content src/rvngetpalver.c -Value $GenCode

$shared = `
    "src/shared_all.c",
"src/rvngetpalver.c"

$win_files = `
    "src/win/fileutils.c",
"src/win/getcurrentthreadid.c",
"src/win/geterrorstring.c",
"src/win/getsysteminformation.c",
"src/win/journal.c",
"src/win/mapping.c",
"src/win/ioring.c",
"src/win/pager.c",
"src/win/virtualmemory.c",
"src/win/writefileheader.c"

$posix_files = `
    "src/posix/fileutils.c",
"src/posix/geterrorstring.c",
"src/posix/getsysteminformation.c",
"src/posix/journal.c",
"src/posix/mapping.c",
"src/posix/pager.c",
"src/posix/sync.c",
"src/posix/virtualmemory.c",
"src/posix/writefileheader.c"

$linux_only = `
    "src/posix/linuxonly.c",
    "src/posix/ioring.c"

Remove-Item .\runtimes -Force -Recurse -ErrorAction Ignore
Remove-Item .\artifacts -Force -Recurse -ErrorAction Ignore

New-Item -ItemType Directory -Path runtimes/win-x86/native -Force > $null
New-Item -ItemType Directory -Path runtimes/win-x64/native -Force > $null
New-Item -ItemType Directory -Path runtimes/linux-x64/native -Force > $null
New-Item -ItemType Directory -Path runtimes/linux-arm/native -Force > $null
New-Item -ItemType Directory -Path runtimes/linux-arm64/native -Force > $null
New-Item -ItemType Directory -Path runtimes/osx-arm64/native -Force > $null


Write-Output "Building Windows x86"
zig cc -Wall -O3 -g -shared -fPIC -Iinc -target x86-windows-gnu -o runtimes/win-x86/native/librvnpal.dll $shared $win_files

Write-Output "Building Windows x64"
zig cc -Wall -O3 -g -shared -fPIC -Iinc -target x86_64-windows-gnu -o runtimes/win-x64/native/librvnpal.dll  $shared $win_files

Write-Output "Building Linux x64"
zig cc -Wall -O3 -g -shared  -fPIC -Iinc -target x86_64-linux-gnu ../../libs/liburing/liburing-2.8.1-x64.a -o runtimes/linux-x64/native/librvnpal.so $shared $posix_files $linux_only

Write-Output "Building Linux ARM32 (Rasbperry Pi)"
zig cc -Wall -O3 -g -shared  -fPIC -Iinc -target arm-linux-gnueabihf -o runtimes/linux-arm/native/librvnpal.so $shared $posix_files $linux_only 

Write-Output "Building Linux ARM64"
zig cc -Wall -O3 -g -shared  -fPIC -Iinc -target aarch64-linux-gnu ../../libs/liburing/liburing-2.8.1-aarch64.a -o runtimes/linux-arm64/native/librvnpal.so $shared $posix_files $linux_only 

Write-Output "Building Linux Mac ARM64"
zig cc -Wall -O3 -g -shared  -fPIC -Iinc -target aarch64-macos-none -o runtimes/osx-arm64/native/librvnpal.dylib $shared $posix_files "src/posix/maconly.c" 

New-Item -ItemType Directory -Path artifacts -Force  > $null
Move-Item .\runtimes artifacts  -ErrorAction Ignore
$PalNuspec = (Get-Content pal.nuspec.template)
$NuspecVersion = "$($PalVerStr[0]).$($PalVerStr[1]).$([convert]::ToInt32($PalVerStr.Substring(2)))"
$PalNuspec = $PalNuspec.Replace("NUGET_PACKAGE_VERSION", $NuspecVersion)
Set-Content artifacts/pal.nuspec  -Value $PalNuspec

# dummy project, required to have donet pack working
Set-Content artifacts/project.csproj -Value "<Project Sdk='Microsoft.NET.Sdk'><PropertyGroup><TargetFramework>netstandard2.0</TargetFramework></PropertyGroup></Project>"

Set-Location artifacts
Remove-Item *.nupkg -ErrorAction Ignore

dotnet pack project.csproj -p:NuspecFile=pal.nuspec -p:NoBuild=true --output . 

Remove-Item ../../../libs/Raven.Pal.* -ErrorAction Ignore
Copy-Item *.nupkg ../../../libs
Set-Location ..
    
$dirPackagesPath = Resolve-Path -Path "../../Directory.Packages.props"
[xml]$dirPkgs = Get-Content $dirPackagesPath
$packageNode = $dirPkgs.Project.ItemGroup.PackageVersion | Where-Object { $_.Include -eq "Raven.Pal" }
$packageNode.Version = $NuspecVersion
$dirPkgs.Save($dirPackagesPath)
Set-Location ..
