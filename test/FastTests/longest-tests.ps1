del test-timings.xml

dotnet xunit -xml test-timings.xml 

[xml]$tests = Get-Content test-timings.xml
$tests.assemblies.assembly.collection.test | 
    sort @{e={$_.time -as [double]} } -descending | 
    select time, name -first 25