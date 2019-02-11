param ( [switch]$zeroversion = $false )
$PalVerStr=(Get-Content pal.ver)
[int]$PalVer=[convert]::ToInt32($PalVerStr, 10)
$PalVer++
if ( $zeroversion -eq $true ) { $PalVer=0 }
else { Set-Content pal.ver -Value $PalVer }
$GenCode="#include <sys/types.h>`n#include ""rvn.h""`nEXPORT int32_t rvn_get_pal_ver() { return  $($PalVer) ; }"
Set-Content src/rvngetpalver.c -Value $GenCode

if (Test-Path artifacts) {
    Del -Recurse .\artifacts\
}

if (-not (Test-Path osxcross)) {
	wsl bash -c './build-all-posix.sh setup'
}
wsl bash -c './build-all-posix.sh build'

cmd.exe /c build-all-windows.bat

copy artifacts\*.* ..\..\libs\librvnpal\
