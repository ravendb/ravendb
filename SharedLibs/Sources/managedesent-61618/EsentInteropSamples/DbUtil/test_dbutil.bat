@echo off

REM A simple test script for the Dbutil executable. This batch
REM file runs the commands and compares the output with the 
REM expected output.

set DATABASE=testing.db
set DBDIR=test
rmdir /s /q bin\debug\%DBDIR%\ 2> nul
mkdir bin\debug\test\
pushd bin\debug\test

echo Passing in bad argumnts...
..\DbUtil nosuchcommand 2>nul
if %ERRORLEVEL%==0 goto :Fail

echo Creating sample database...
..\DbUtil createsample %DATABASE%
if NOT %ERRORLEVEL%==0 goto :Fail

echo Dumping metadata...
..\Dbutil dumpmetadata %DATABASE% > metadata.txt
if NOT %ERRORLEVEL%==0 goto :Fail

echo n | comp /a metadata.txt ..\..\..\tests\expected_metadata.txt  1>nul 2>nul
if NOT %ERRORLEVEL%==0 goto :Fail

echo Dumping table to csv format...
..\Dbutil dumptocsv %DATABASE% table > table.csv
echo n | comp /a table.csv ..\..\..\tests\expected_table.csv  1>nul 2>nul
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

