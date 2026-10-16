param(
    [int]$Runs = 1,
    [int]$Top = 25,
    [string]$Configuration = "Release",
    [string]$OutputDir = (Join-Path $PSScriptRoot "..\..\.agents\test-timings"),
    # run test collections in parallel (as 'dotnet test' does); by default tests run serially so their timings do not affect each other
    [switch]$Parallel,
    # only aggregate the result files already in $OutputDir
    [switch]$SkipRun
)

$ErrorActionPreference = "Stop"

New-Item -ItemType Directory -Force $OutputDir | Out-Null
$OutputDir = (Resolve-Path $OutputDir).Path

if ($SkipRun -eq $false) {
    dotnet build $PSScriptRoot --configuration $Configuration
    if ($LASTEXITCODE -ne 0) { throw "Build failed" }

    $assembly = Join-Path $PSScriptRoot "bin\$Configuration\net10.0\FastTests.dll"
    $parallelism = if ($Parallel) { "collections" } else { "none" }

    Get-ChildItem $OutputDir -Filter "run-*.xml" | Remove-Item

    for ($i = 1; $i -le $Runs; $i++) {
        $resultsFile = Join-Path $OutputDir "run-$i.xml"

        # xUnit.net v3 test projects are self-executing - the 'dotnet xunit' tool no longer exists.
        # Failing tests do not stop the measurement, so the exit code is ignored.
        # '-parallelMode' must be used, the deprecated '-parallel' switch is ignored
        dotnet $assembly -parallelMode $parallelism -noColor -xml $resultsFile
    }
}

$resultFiles = @(Get-ChildItem $OutputDir -Filter "run-*.xml")
if ($resultFiles.Count -eq 0) { throw "No result files found in $OutputDir" }

# a theory is moved as a whole, so its rows are summed per method; theory rows can carry random data
# in their display names, so runs are compared per method and the fastest run is taken to filter out noise
$tests = @{}
foreach ($resultFile in $resultFiles) {
    [xml]$results = Get-Content $resultFile.FullName
    $results.assemblies.assembly.collection.test |
        Group-Object type, method |
        ForEach-Object {
            $time = ($_.Group | ForEach-Object { $_.time -as [double] } | Measure-Object -Sum).Sum
            $existing = $tests[$_.Name]
            if ($null -eq $existing) {
                $tests[$_.Name] = [pscustomobject]@{ Type = $_.Group[0].type; Method = $_.Group[0].method; Rows = $_.Count; Time = $time }
            }
            elseif ($time -lt $existing.Time) {
                $existing.Time = $time
                $existing.Rows = $_.Count
            }
        }
}

$totalTime = ($tests.Values | Measure-Object Time -Sum).Sum

$methods = $tests.Values |
    ForEach-Object {
        [pscustomobject]@{
            Seconds = [math]::Round($_.Time, 2)
            Percent = [math]::Round(100 * $_.Time / $totalTime, 2)
            Rows = $_.Rows
            Type = $_.Type
            Method = $_.Method
        }
    } |
    Sort-Object Seconds -Descending

$classes = $methods |
    Group-Object Type |
    ForEach-Object {
        $time = ($_.Group | Measure-Object Seconds -Sum).Sum
        $slowest = $_.Group | Sort-Object Seconds -Descending | Select-Object -First 1
        [pscustomobject]@{
            Seconds = [math]::Round($time, 2)
            Percent = [math]::Round(100 * $time / $totalTime, 2)
            Methods = $_.Count
            SlowestMethodSeconds = $slowest.Seconds
            Type = $_.Name
        }
    } |
    Sort-Object Seconds -Descending

$methods | Export-Csv (Join-Path $OutputDir "methods.csv") -NoTypeInformation
$classes | Export-Csv (Join-Path $OutputDir "classes.csv") -NoTypeInformation

Write-Host ("{0} test methods, {1} runs, total {2:N0}s (fastest run per method)" -f $tests.Count, $resultFiles.Count, $totalTime)
Write-Host "Slowest methods:"
$methods | Select-Object -First $Top | Format-Table -AutoSize
Write-Host "Slowest classes:"
$classes | Select-Object -First $Top | Format-Table -AutoSize
Write-Host "Full results: $OutputDir"
