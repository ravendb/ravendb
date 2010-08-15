@echo off
pushd %~dp0
..\..\bin\ir.exe -S rake run
set E=%ERRORLEVEL%
popd
exit /B %E%
:END
