@echo off
setlocal
cd /d "%~dp0"
if not exist "artifacts" mkdir "artifacts"
for %%p in (win.x86 win.x64 win.arm64 win7.x86 win7.x64) do if exist artifacts\librvnpal.%%p.dll del artifacts\librvnpal.%%p.dll

rem The latest Visual Studio with the C++ x86/x64 build tools; win-arm64 also needs the 'MSVC ARM64 build tools' component.
rem cl comes from the PATH that vcvars sets, so a new MSVC version needs no change here.
set "vswhere=%ProgramFiles(x86)%\Microsoft Visual Studio\Installer\vswhere.exe"
if not exist "%vswhere%" goto :novs
set "vsroot="
for /f "usebackq delims=" %%i in (`"%vswhere%" -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath`) do set "vsroot=%%i"
if not defined vsroot goto :novs
set "vcbin=%vsroot%\VC\Auxiliary\Build"
echo = Visual Studio: %vsroot%

set "win7=/D RVN_WIN7 /D WINVER=0x0601 /D _WIN32_WINNT=0x0601 /D NTDDI_VERSION=0x6010000 /D WINNT=1"

call :build vcvars32.bat librvnpal.win.x86.dll
call :build vcvars64.bat librvnpal.win.x64.dll
rem there is no win7 variant for arm64, Windows 7 never ran on ARM64
call :build vcvarsamd64_arm64.bat librvnpal.win.arm64.dll
call :build vcvars32.bat librvnpal.win7.x86.dll "%win7%"
call :build vcvars64.bat librvnpal.win7.x64.dll "%win7%"

set failed=0
echo ===================================================
for %%p in (win.x86 win.x64 win.arm64 win7.x86 win7.x64) do (
	if exist artifacts\librvnpal.%%p.dll (
		echo = Build librvnpal.%%p.dll : SUCCESS
	) else (
		echo = Build librvnpal.%%p.dll : FAIL
		set failed=1
	)
)
echo ===================================================
exit /b %failed%

rem %1 vcvars script, %2 output dll, %3 additional compiler flags
:build
echo = Building %~2 (%~1)
rem a dll left by an interrupted run must not pass for this build
if exist %~2 del /q %~2
cmd.exe /c ""%vcbin%\%~1" >nul & cl -Fe%~2 -I inc %~3 /O2 /analyze /sdl /LD src\*.c src\win\*.c /link"
rem cmd /c returns the exit code of cl
if not errorlevel 1 if exist %~2 move /y %~2 artifacts\%~2 >nul
del /q *.obj %~n2.lib %~n2.exp %~2 2>nul
exit /b 0

:novs
echo = Visual Studio with the C++ build tools was not found (vswhere: %vswhere%)
exit /b 1
