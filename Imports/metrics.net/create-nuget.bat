rd /S /Q .\Publishing\lib

call build.bat
if %errorlevel% neq 0 exit /b %errorlevel%

md .\Publishing\lib
md .\Publishing\lib\net45

copy .\bin\Release\Metrics.dll .\Publishing\lib\net45\
copy .\bin\Release\Metrics.xml .\Publishing\lib\net45\
copy .\bin\Release\Metrics.pdb .\Publishing\lib\net45\

copy .\bin\Release\Nancy.metrics.dll .\Publishing\lib\net45\
copy .\bin\Release\Nancy.metrics.xml .\Publishing\lib\net45\
copy .\bin\Release\Nancy.metrics.pdb .\Publishing\lib\net45\

copy .\bin\Release\owin.metrics.dll .\Publishing\lib\net45\
copy .\bin\Release\owin.metrics.xml .\Publishing\lib\net45\
copy .\bin\Release\owin.metrics.pdb .\Publishing\lib\net45\

.\.nuget\NuGet.exe pack .\Publishing\Metrics.Net.nuspec -OutputDirectory .\Publishing
if %errorlevel% neq 0 exit /b %errorlevel%

.\.nuget\NuGet.exe pack .\Publishing\Nancy.Metrics.nuspec -OutputDirectory .\Publishing
if %errorlevel% neq 0 exit /b %errorlevel%

.\.nuget\NuGet.exe pack .\Publishing\Owin.Metrics.nuspec -OutputDirectory .\Publishing
if %errorlevel% neq 0 exit /b %errorlevel%