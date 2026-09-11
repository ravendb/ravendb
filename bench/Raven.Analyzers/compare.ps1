#Requires -Version 7
<#
.SYNOPSIS
    Compares build time with and without the RavenDB Roslyn analyzers.

.DESCRIPTION
    Runs N clean builds of each case and takes the median, so a single slow or
    fast build does not skew the result. The two cases are interleaved
    (without, with, without, with, ...) so a transient IO or CPU hiccup tends to
    hit both cases rather than skewing just one of them.

    Prints the median wall-clock for each case, the delta + percentage, the
    median reported analyzer execution time, the per-run samples, and the top 3
    most expensive analyzers from the representative (median) run.

.PARAMETER Configuration
    MSBuild configuration to use. Default: Release.

.PARAMETER Runs
    Number of clean builds per case. The median is reported. Default: 5.
#>
[CmdletBinding()]
param(
    [string]$Configuration = 'Release',
    [int]$Runs = 5
)

$ErrorActionPreference = 'Stop'
$proj = Join-Path $PSScriptRoot 'Raven.Analyzers.csproj'

function Get-Median {
    param([double[]]$Values)

    if ($null -eq $Values -or $Values.Count -eq 0) {
        return $null
    }

    $sorted = $Values | Sort-Object
    $n = $sorted.Count
    if ($n % 2 -eq 1) {
        return $sorted[[int][math]::Floor($n / 2)]
    }

    return ($sorted[$n / 2 - 1] + $sorted[$n / 2]) / 2
}

function Invoke-CleanBuild {
    param([bool]$UseAnalyzers)

    $flag = if ($UseAnalyzers) { 'true' } else { 'false' }

    # Clean first so no run benefits from the incremental cache.
    dotnet build $proj -c $Configuration /p:UseRavenAnalyzers=$flag /t:Clean `
        --verbosity quiet | Out-Null

    $sw     = [System.Diagnostics.Stopwatch]::StartNew()
    $output = dotnet build $proj -c $Configuration /p:UseRavenAnalyzers=$flag `
                  --verbosity normal 2>&1
    $sw.Stop()

    # Parse "Total analyzer execution time: X.XXX seconds." from ReportAnalyzer output.
    $analyzerSeconds = $null
    $totalLine = $output | Select-String -Pattern 'Total analyzer execution time:\s+([0-9.]+)\s+seconds'
    if ($totalLine) {
        $analyzerSeconds = [double]$totalLine.Matches[0].Groups[1].Value
    }

    # Collect individual analyzer rows (tab-indented, start with a number).
    $analyzerRows = $output |
        Select-String -Pattern '^\s+[0-9]+\.[0-9]+\s+[0-9]+\s+Raven\.Analyzers\.' |
        ForEach-Object { $_.Line.Trim() }

    [pscustomobject]@{
        Wallclock       = $sw.Elapsed.TotalSeconds
        AnalyzerSeconds = $analyzerSeconds
        AnalyzerRows    = $analyzerRows
    }
}

function Get-Aggregate {
    param([object[]]$Results)

    [double[]]$wall = $Results | ForEach-Object { $_.Wallclock }
    $medianWall = Get-Median -Values $wall

    [double[]]$analyzerVals = $Results |
        Where-Object { $null -ne $_.AnalyzerSeconds } |
        ForEach-Object { $_.AnalyzerSeconds }
    $medianAnalyzer = Get-Median -Values $analyzerVals

    # Representative run: the one whose wall-clock is closest to the median.
    $repRun = $Results | Sort-Object { [math]::Abs($_.Wallclock - $medianWall) } | Select-Object -First 1

    [pscustomobject]@{
        Wallclock       = $medianWall
        WallSamples     = $wall
        AnalyzerSeconds = $medianAnalyzer
        AnalyzerRows    = $repRun.AnalyzerRows
    }
}

function Format-Samples {
    param([double[]]$Values)
    return (($Values | ForEach-Object { '{0:N3}' -f $_ }) -join ', ')
}

# Interleave the two cases (without, with, without, with, ...) so a transient
# IO or CPU hiccup tends to hit both rather than skewing just one of them.
Write-Host ("Benchmarking, interleaving {0} runs per case (without, with, ...)" -f $Runs) -ForegroundColor Cyan

$baselineResults = @()
$withResults     = @()
for ($i = 1; $i -le $Runs; $i++) {
    Write-Host ("  run {0}/{1}: without analyzers..." -f $i, $Runs) -ForegroundColor DarkGray
    $baselineResults += Invoke-CleanBuild -UseAnalyzers:$false
    Write-Host ("  run {0}/{1}: with analyzers..." -f $i, $Runs) -ForegroundColor DarkGray
    $withResults += Invoke-CleanBuild -UseAnalyzers:$true
}

$baseline      = Get-Aggregate -Results $baselineResults
$withAnalyzers = Get-Aggregate -Results $withResults

$deltaSec = $withAnalyzers.Wallclock - $baseline.Wallclock
$deltaPct = if ($baseline.Wallclock -gt 0) { ($deltaSec / $baseline.Wallclock) * 100 } else { 0 }

Write-Host ''
Write-Host ("=== Build timing comparison (median of {0} runs) ===" -f $Runs) -ForegroundColor Green
Write-Host ('{0,-42} {1,8:N3} s' -f 'Median wall-clock, without analyzers', $baseline.Wallclock)
Write-Host ('{0,-42} {1,8:N3} s' -f 'Median wall-clock, with analyzers',    $withAnalyzers.Wallclock)
Write-Host ('{0,-42} {1,8:N3} s  ({2:+0.0;-0.0}%)' -f 'Median wall-clock delta', $deltaSec, $deltaPct)

if ($null -ne $withAnalyzers.AnalyzerSeconds) {
    Write-Host ('{0,-42} {1,8:N3} s' -f 'Median reported analyzer time', $withAnalyzers.AnalyzerSeconds)
}

Write-Host ''
Write-Host ('{0,-42} {1}' -f 'Baseline samples (s)',       (Format-Samples -Values $baseline.WallSamples))
Write-Host ('{0,-42} {1}' -f 'With-analyzers samples (s)', (Format-Samples -Values $withAnalyzers.WallSamples))

$top3 = $withAnalyzers.AnalyzerRows | Select-Object -First 3
if ($top3) {
    Write-Host ''
    Write-Host 'Top analyzers by cost (median run):' -ForegroundColor Yellow
    $top3 | ForEach-Object { Write-Host "  $_" }
}

# Write a machine-readable result. 'success' is the gate: the relative overhead the
# analyzers add to a cold build must stay under this threshold.
$overheadThresholdPercent = 20
$success = $deltaPct -lt $overheadThresholdPercent

$result = [ordered]@{
    without          = [math]::Round($baseline.Wallclock, 3)
    with             = [math]::Round($withAnalyzers.Wallclock, 3)
    overheadAbsolute = [math]::Round($deltaSec, 3)
    overheadRelative = [math]::Round($deltaPct, 2)
    success          = $success
}

$jsonPath = Join-Path $PSScriptRoot 'analyzers-benchmark-result.json'
$result | ConvertTo-Json | Set-Content -Path $jsonPath -Encoding utf8

Write-Host ''
Write-Host ('Result written to {0}' -f $jsonPath) -ForegroundColor Green
$gate = if ($success) { 'PASS' } else { 'FAIL' }
$gateColor = if ($success) { 'Green' } else { 'Red' }
Write-Host ('  {0}: relative overhead {1:N1}% (threshold {2}%)' -f $gate, $deltaPct, $overheadThresholdPercent) -ForegroundColor $gateColor

Write-Host ''
