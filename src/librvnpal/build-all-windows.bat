if not exist "artifacts" mkdir "artifacts"

cmd.exe /c ""C:\Program Files (x86)\Microsoft Visual Studio\2017\BuildTools\Common7\Tools\VsDevCmd.bat" & "C:\Program Files (x86)\Microsoft Visual Studio\2017\BuildTools\VC\Auxiliary\Build\vcvars32.bat" & cl -Felibrvnpal.x86.dll -I inc /O2 /sdl /experimental:external /external:anglebrackets /external:W0 /Wall /LD src\*.c src\win\*.c /link"
copy librvnpal.x86.dll artifacts\librvnpal.win.x86.dll
del *.obj librvnpal.x*

cmd.exe /c ""C:\Program Files (x86)\Microsoft Visual Studio\2017\BuildTools\Common7\Tools\VsDevCmd.bat" & "C:\Program Files (x86)\Microsoft Visual Studio\2017\BuildTools\VC\Auxiliary\Build\vcvars64.bat" & cl -Felibrvnpal.x64.dll -I inc /O2 /sdl /experimental:external /external:anglebrackets /external:W0 /Wall /LD src\*.c src\win\*.c /link"
copy librvnpal.x64.dll artifacts\librvnpal.win.x64.dll
del *.obj librvnpal.x*
