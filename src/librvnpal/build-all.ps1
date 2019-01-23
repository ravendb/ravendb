if (Test-Path artifacts) {
    Del -Recurse .\artifacts\
}

if (-not (Test-Path osxcross)) {
	wsl bash -c './build-all-posix.sh setup'
}
wsl bash -c './build-all-posix.sh build'

cmd.exe /c build-all-windows.bat

copy artifacts\*.* ..\..\libs\librvnpal\
