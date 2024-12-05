param ( [switch]$skip_version_increment = $false, [switch]$clang_only = $false )


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

$vcbin = "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build"
$clbin = (gci "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Tools\MSVC\" | sort Name -Descending | select -First 1).FullName
$clbin = "$clbin\bin\Hostx64"

Remove-Item .\runtimes -Force -Recurse -ErrorAction Ignore
Remove-Item .\artifacts -Force -Recurse -ErrorAction Ignore

mkdir runtimes/win-x86/native -ErrorAction Ignore > $null
Copy-Item ../../libs/libzstd/libzstd.win.x86.dll runtimes/win-x86/native/libzstd.dll
mkdir runtimes/win-x64/native -ErrorAction Ignore > $null
Copy-Item ../../libs/libzstd/libzstd.win.x64.dll runtimes/win-x64/native/libzstd.dll
mkdir runtimes/linux-x64/native -ErrorAction Ignore > $null
Copy-Item ../../libs/libzstd/libzstd.linux.x64.so runtimes/linux-x64/native/libzstd.so
mkdir runtimes/linux-arm/native -ErrorAction Ignore > $null
Copy-Item ../../libs/libzstd/libzstd.linux.arm.32.so runtimes/linux-arm/native/libzstd.so
mkdir runtimes/linux-arm64/native -ErrorAction Ignore > $null
Copy-Item ../../libs/libzstd/libzstd.linux.arm.64.so runtimes/linux-arm64/native/libzstd.so
mkdir runtimes/osx-x64/native -ErrorAction Ignore > $null
Copy-Item ../../libs/libzstd/libzstd.mac.x64.dylib runtimes/osx-x64/native/libzstd.dylib
mkdir runtimes/osx-arm64/native -ErrorAction Ignore > $null
Copy-Item ../../libs/libzstd/libzstd.mac.arm64.dylib runtimes/osx-arm64/native/libzstd.dylib


Write-Output "Building Windows x86"
if ($clang_only) {
    zig cc -Wall -O3 -g -shared -fPIC -Iinc -target x86-windows -o runtimes/win-x86/native/librvnpal.dll $shared $win_files 
}
else {
    cmd /c """$vcbin\vcvars32.bat"" & ""$clbin\x86\cl"" -Feruntimes/win-x86/native/librvnpal.dll -I inc /O2 /analyze /sdl /LD $shared $win_files /link"
    Remove-Item *.obj
    Remove-Item  librvnpal.x*
    Remove-Item *.nativecodeanalysis.xml        
}

Write-Output "Building Windows x64"
if ($clang_only) {
    zig cc -Wall -O3 -g -shared -fPIC -Iinc -target x86_64-windows -o runtimes/win-x64/native/librvnpal.dll  $shared $win_files 
}
else {
    cmd /c """$vcbin\vcvars64.bat"" & ""$clbin\x64\cl"" -Feruntimes/win-x64/native/librvnpal.dll -I inc /O2 /analyze /sdl /LD $shared $win_files /link"
    Remove-Item *.obj
    Remove-Item  librvnpal.x*
    Remove-Item *.nativecodeanalysis.xml
}

Write-Output "Building Linux x64"
zig cc -Wall -O3 -g -shared  -fPIC -Iinc -target x86_64-linux-gnu ../../libs/liburing/liburing-2.8.1-x64.a -o runtimes/linux-x64/native/librvnpal.so $shared $posix_files "src/posix/linuxonly.c" 

Write-Output "Building Linux ARM32 (Rasbperry Pi)"
zig cc -Wall -O3 -g -shared  -fPIC -Iinc -target arm-linux-gnueabihf -o runtimes/linux-arm/native/librvnpal.so $shared $posix_files "src/posix/linuxonly.c" 

Write-Output "Building Linux ARM64"
zig cc -Wall -O3 -g -shared  -fPIC -Iinc -target aarch64-linux-gnu ../../libs/liburing/liburing-2.8.1-aarch64.a -o runtimes/linux-arm64/native/librvnpal.so $shared $posix_files "src/posix/linuxonly.c" 

Write-Output "Building Linux Mac x64"
zig cc -Wall -O3 -g -shared -fPIC -Iinc -target x86_64-macos-none -o runtimes/osx-x64/native/librvnpal.dylib $shared $posix_files "src/posix/maconly.c" 

Write-Output "Building Linux Mac ARM64"
zig cc -Wall -O3 -g -shared  -fPIC -Iinc -target aarch64-macos-none -o runtimes/osx-arm64/native/librvnpal.dylib $shared $posix_files "src/posix/maconly.c" 

mkdir artifacts  -ErrorAction Ignore  > $null
Move-Item .\runtimes artifacts  -ErrorAction Ignore
$PalNuspec = (Get-Content pal.nuspec.template)
$NuspecVersion = "$($PalVerStr[0]).$($PalVerStr[1]).$($PalVerStr.Substring(2).TrimStart('0'))"
$PalNuspec = $PalNuspec.Replace("NUGET_PACKAGE_VERSION", $NuspecVersion)
Set-Content artifacts\pal.nuspec  -Value $PalNuspec

Set-Location artifacts
Remove-Item *.nupkg
../../../scripts/assets/bin/nuget.exe pack .\pal.nuspec
Remove-Item ../../../libs/Raven.Pal.*
Copy-Item *.nupkg ../../../libs
Set-Location ..

$dirPackagesPath = Resolve-Path -Path "../../Directory.Packages.props"
[xml]$dirPkgs = Get-Content $dirPackagesPath
$packageNode = $dirPkgs.Project.ItemGroup.PackageVersion | Where-Object { $_.Include -eq "Raven.Pal" }
$packageNode.Version = $NuspecVersion
$dirPkgs.Save($dirPackagesPath)
