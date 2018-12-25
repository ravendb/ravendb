ECHO OFF
cls
ECHO Execute from Developer Command Pronmpt for VS 2017
ECHO Make sure to Clean all before generating libs
ECHO.
:MENU
ECHO.
ECHO ...............................................
ECHO PRESS 1, 2 OR 3 to select your task, or 4 to EXIT.
ECHO ...............................................
ECHO.
ECHO REMINDER : You must OPEN NEW developer cmd prompt for each arch, and CLEAN before generating lib !
ECHO.
ECHO 1 - Build for Win-X86 and copy to ..\..\libs\librvnpal\
ECHO 2 - Build for Win-X64 and copy to ..\..\libs\librvnpal\
ECHO 3 - Clean all
ECHO 4 - EXIT
ECHO.
SET /P M=Type 1, 2, 3, or 4 then press ENTER:
IF %M%==1 GOTO WINX86
IF %M%==2 GOTO WINX64
IF %M%==3 GOTO CLEAN
IF %M%==4 GOTO EOF
:WINX86
call "C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\VC\Auxiliary\Build\vcvars32.bat"
cl -Felibrvnpal.x86.dll -I inc /O2 /sdl /experimental:external /external:anglebrackets /external:W0 /Wall /LD src\win\*.c /link
copy /Y librvnpal.x86.dll ..\..\libs\librvnpal\librvnpal.win.x86.dll
GOTO MENU
:WINX64
call "C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\VC\Auxiliary\Build\vcvars64.bat"
cl -Felibrvnpal.x64.dll -I inc /O2 /sdl /experimental:external /external:anglebrackets /external:W0 /Wall /LD src\win\*.c /link
copy /Y librvnpal.x64.dll ..\..\libs\librvnpal\librvnpal.win.x64.dll
GOTO MENU
:CLEAN
del *.obj librvnpal.x*
GOTO MENU
:EOF
ECHO Bye bye..
