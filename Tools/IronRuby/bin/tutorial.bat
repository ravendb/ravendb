@echo off

if defined DLR_ROOT (
  REM - This is a dev environment. See http://wiki.github.com/ironruby/ironruby
  call "%DLR_ROOT%\Languages\Ruby\Samples\Tutorial\tutorial.bat"
) else (
  call "%~dp0..\Samples\Tutorial\tutorial.bat"
)

