$resultsFile = "test-timings.xml"

if (Test-Path $resultsFile) { del $resultsFile }

# xUnit.net v3 test projects are self-executing - the 'dotnet xunit' tool no longer exists.
dotnet run --configuration Release -- -result-xml $resultsFile

[xml]$tests = Get-Content $resultsFile
$tests.assemblies.assembly.collection.test | 
    sort @{e={$_.time -as [double]} } -descending | 
    select time, name -first 25