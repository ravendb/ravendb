@echo off

REM A simple test script for the StockSample executable. This batch
REM file runs the commands and compares the output with the 
REM expected output.

set DBDIR=test
rmdir /s /q bin\debug\%DBDIR%\ 2> nul
mkdir bin\debug\test\
pushd bin\debug\test

echo Running StockSample...
..\StockSample > output.txt
if NOT %ERRORLEVEL%==0 goto :Fail

echo n | comp /a output.txt ..\..\..\expected_output.txt  1>nul 2>nul
if NOT %ERRORLEVEL%==0 goto :Fail

popd
rmdir /s /q bin\debug\%DBDIR%\

echo.
echo **********************************
echo Test Passed!
echo **********************************

goto :EOF

:Fail
echo.
echo **********************************
echo Test failed!
echo **********************************

