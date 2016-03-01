mono .nuget/NuGet.exe restore Metrics.sln 

xbuild Metrics.Sln /p:Configuration="Debug"
xbuild Metrics.Sln /p:Configuration="Release"

mono ./packages/xunit.runners.1.9.2/tools/xunit.console.clr4.exe ./bin/Debug/Tests/Metrics.Tests.dll -parallel none
mono ./packages/xunit.runners.1.9.2/tools/xunit.console.clr4.exe ./bin/Release/Tests/Metrics.Tests.dll -parallel none