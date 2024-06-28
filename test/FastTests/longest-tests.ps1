dotnet test --logger "trx;LogFileName=output.xml"

[xml]$tests = Get-Content .\TestResults\output.xml
$tests.TestRun.Results.UnitTestResult | sort @{e={$_.duration} } -descending | select testName, duration -first 25