@echo off
if not exist "artifacts" mkdir "artifacts"
if exist artifacts\librvnpal.x64.dll del artifacts\librvnpal.x64.dll
if exist artifacts\librvnpal.x86.dll del artifacts\librvnpal.x86.dll


set vcbin=C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build
set clbin=C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Tools\MSVC\14.40.33807\bin\Hostx64

cmd.exe /c ""%vcbin%\vcvars32.bat" & "%clbin%\x86\cl" -Felibrvnpal.x86.dll -I inc /O2 /analyze /sdl /LD src\*.c src\win\*.c /link"
copy librvnpal.x86.dll artifacts\librvnpal.win.x86.dll
del *.obj librvnpal.x*

cmd.exe /c ""%vcbin%\vcvars64.bat" & "%clbin%\x64\cl" -Felibrvnpal.x64.dll -I inc /O2 /analyze /sdl /LD src\*.c src\win\*.c /link"
copy librvnpal.x64.dll artifacts\librvnpal.win.x64.dll
del *.obj librvnpal.x*

cmd.exe /c ""%vcbin%\vcvars32.bat" & "%clbin%\x86\cl" -Felibrvnpal.win7x86.dll -I inc /D RVN_WIN7 /D WINVER=0x0601 /D _WIN32_WINNT=0x0601 /D NTDDI_VERSION=0x6010000 /D WINNT=1 /O2 /analyze /sdl /LD src\*.c src\win\*.c /link"
copy librvnpal.win7x86.dll artifacts\librvnpal.win7.x86.dll
del *.obj librvnpal.win7x*

cmd.exe /c ""%vcbin%\vcvars64.bat" & "%clbin%\x64\cl" -Felibrvnpal.win7x64.dll -I inc /D RVN_WIN7 /D WINVER=0x0601 /D _WIN32_WINNT=0x0601 /D NTDDI_VERSION=0x6010000 /D WINNT=1 /O2 /analyze /sdl /LD src\*.c src\win\*.c /link"
copy librvnpal.win7x64.dll artifacts\librvnpal.win7.x64.dll
del *.obj librvnpal.win7x*


echo ===================================================
if exist artifacts\librvnpal.win.x64.dll (
	echo = Build win-x64 librvnpal.win.x64.dll   : SUCCESS =
) else (
	echo = Build win-x64 librvnpal.win.x64.dll   : FAIL    =
)
if exist artifacts\librvnpal.win.x86.dll (
	echo = Build win-x86 librvnpal.win.x86.dll   : SUCCESS =
) else (
	echo = Build win-x86 librvnpal.win.x86.dll   : FAIL    =
)
if exist artifacts\librvnpal.win7.x64.dll (
	echo = Build win7-x64 librvnpal.win7.x64.dll : SUCCESS =
) else (
	echo = Build win7-x64 librvnpal.win7.x64.dll : FAIL    =
)
if exist artifacts\librvnpal.win7.x86.dll (
	echo = Build win7-x86 librvnpal.win7.x86.dll : SUCCESS =
) else (
	echo = Build win7-x86 librvnpal.win7.x86.dll : FAIL    =
)
echo ===================================================





