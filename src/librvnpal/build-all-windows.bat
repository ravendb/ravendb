@echo off
if not exist "artifacts" mkdir "artifacts"
if exist artifacts\librvnpal.x64.dll del artifacts\librvnpal.x64.dll
if exist artifacts\librvnpal.x86.dll del artifacts\librvnpal.x86.dll

set vcbin=C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\bin

cmd.exe /c ""%vcbin%\vcvars32.bat" & "%vcbin%\cl" -Felibrvnpal.x86.dll -I inc /O2 /sdl /LD src\*.c src\win\*.c /link"
copy librvnpal.x86.dll artifacts\librvnpal.win.x86.dll
del *.obj librvnpal.x*

cmd.exe /c ""%vcbin%\amd64\vcvars64.bat" & "%vcbin%\amd64\cl" -Felibrvnpal.x64.dll -I inc /O2 /sdl /LD src\*.c src\win\*.c /link"
copy librvnpal.x64.dll artifacts\librvnpal.win.x64.dll
del *.obj librvnpal.x*

echo =================================================
if exist artifacts\librvnpal.win.x64.dll (
	echo = Build win-x64 librvnpal.win.x64.dll : SUCCESS =
) else (
	echo = Build win-x64 librvnpal.win.x64.dll : FAIL    =
)
if exist artifacts\librvnpal.win.x86.dll (
	echo = Build win-x86 librvnpal.win.x86.dll : SUCCESS =
) else (
	echo = Build win-x86 librvnpal.win.x86.dll : FAIL    =
)
echo =================================================





