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
    "src/fileutils_all.c",
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

mkdir runtimes/win-x86/native -ErrorAction Ignore > $null
Copy-Item ../../libs/libzstd/libzstd.win.x86.dll runtimes/win-x86/native/libzstd.dll
mkdir runtimes/win-x64/native -ErrorAction Ignore > $null
Copy-Item ../../libs/libzstd/libzstd.win.x64.dll runtimes/win-x64/native/libzstd.dll
mkdir runtimes/linux-x86/native -ErrorAction Ignore > $null
# we don't have such a file - that said, I don't know we _have_ a x86 release for Linux
# cp ../../libs/libzstd/libzstd.linux.x86.so runtimes/linux-x86/native/libzstd.so
mkdir runtimes/linux-x64/native -ErrorAction Ignore > $null
Copy-Item ../../libs/libzstd/libzstd.linux.x64.so runtimes/linux-x64/native/libzstd.so
mkdir runtimes/linux-arm/native -ErrorAction Ignore > $null
Copy-Item ../../libs/libzstd/libzstd.linux.arm.32.so runtimes/linux-arm/native/libzstd.so
mkdir runtimes/linux-arm64/native -ErrorAction Ignore > $null
Copy-Item ../../libs/libzstd/libzstd.linux.arm.64.so runtimes/linux-arm64/native/libzstd.so
mkdir runtimes/osx-x64/native -ErrorAction Ignore > $null
Copy-Item ../../libs/libzstd/libzstd.mac.x64.dylib runtimes/osx-x64/native/libzstd.so
mkdir runtimes/osx-arm64/native -ErrorAction Ignore > $null
Copy-Item ../../libs/libzstd/libzstd.mac.arm64.dylib runtimes/osx-arm64/native/libzstd.so


Write-Output "Building Windows x86"
zig cc -Wall -O3  -shared -fPIC -Iinc -target x86-windows -o runtimes/win-x86/native/librvnpal.dll $shared $win_files 

Write-Output "Building Windows x64"
zig cc -Wall -O3  -shared -fPIC -Iinc -target x86_64-windows -o runtimes/win-x64/native/librvnpal.dll  $shared $win_files 

Write-Output "Building Linux x86"
zig cc -Wall -O3  -shared  -fPIC -Iinc -target x86-linux-gnu -o runtimes/linux-x86/native/librvnpal.so  $shared $posix_files "src/posix/linuxonly.c" 

Write-Output "Building Linux x64"
zig cc -Wall -O3  -shared  -fPIC -Iinc -target x86_64-linux-gnu -o runtimes/linux-x64/native/librvnpal.so $shared $posix_files "src/posix/linuxonly.c" 

Write-Output "Building Linux ARM32 (Rasbperry Pi)"
zig cc -Wall -O3  -shared  -fPIC -Iinc -target arm-linux-gnueabihf -o runtimes/linux-arm/native/librvnpal.so $shared $posix_files "src/posix/linuxonly.c" 

Write-Output "Building Linux ARM64"
zig cc -Wall -O3  -shared  -fPIC -Iinc -target aarch64-linux-gnu -o runtimes/linux-arm64/native/librvnpal.so $shared $posix_files "src/posix/linuxonly.c" 

Write-Output "Building Linux Mac x64"
zig cc -Wall -O3  -shared -fPIC -Iinc -target x86_64-macos-none -o runtimes/osx-x64/native/librvnpal.dylib $shared $posix_files "src/posix/maconly.c" 

Write-Output "Building Linux Mac ARM64"
zig cc -Wall -O3  -shared  -fPIC -Iinc -target aarch64-macos-none -o runtimes/osx-arm64/native/librvnpal.dylib $shared $posix_files "src/posix/maconly.c" 

mkdir artifacts  -ErrorAction Ignore  > $null
Move-Item .\runtimes artifacts  -ErrorAction Ignore
$PalNuspec = (Get-Content pal.nuspec.template)
$NuspecVersion = "$($PalVerStr[0]).$($PalVerStr[1]).$($PalVerStr.Substring(2))"
$PalNuspec = $PalNuspec.Replace("NUGET_PACKAGE_VERSION", $NuspecVersion)
Set-Content artifacts\pal.nuspec  -Value $PalNuspec

Set-Location artifacts
Remove-Item *.nupkg
../../../scripts/assets/bin/nuget.exe pack .\pal.nuspec
Remove-Item ../../../libs/RavenDB.Pal.*
Copy-Item *.nupkg ../../../libs
Set-Location ..
dotnet remove ../Sparrow.Server/Sparrow.Server.csproj package RavenDB.Pal
dotnet add ../Sparrow.Server/Sparrow.Server.csproj package RavenDB.Pal --source ..\..\libs\