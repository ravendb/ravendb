dotnet test  -xml test-timings.xml -verbose -parallel none

[xml]$tests = Get-Content test-timings.xml
$tests.assemblies.assembly.collection.test | 
    sort @{e={$_.time -as [double]} } -descending | 
    select time, name -first 10

