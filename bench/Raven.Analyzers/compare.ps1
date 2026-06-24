#Requires -Version 7
<#
.SYNOPSIS
    Compares build time with and without the RavenDB Roslyn analyzers.

.DESCRIPTION
    Runs two clean builds of the playground project:
      1. Without analyzers  (baseline)
      2. With analyzers     (default)

    Prints wall-clock for each, the delta + percentage, the reported total
    analyzer execution time, and the top 3 most expensive analyzers.

.PARAMETER Configuration
    MSBuild configuration to use. Default: Release.
#>
[CmdletBinding()]
param(
    [string]$Configuration = 'Release'
)

$ErrorActionPreference = 'Stop'
$proj = Join-Path $PSScriptRoot 'Raven.Analyzers.csproj'

function Invoke-CleanBuild {
    param([bool]$UseAnalyzers)

    $flag  = if ($UseAnalyzers) { 'true' } else { 'false' }
    $label = if ($UseAnalyzers) { 'WITH analyzers' } else { 'WITHOUT analyzers (baseline)' }

    Write-Host "Building $label..." -ForegroundColor Cyan

    # Clean first so neither run benefits from incremental cache.
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
        UseAnalyzers    = $UseAnalyzers
        Wallclock       = $sw.Elapsed.TotalSeconds
        AnalyzerSeconds = $analyzerSeconds
        AnalyzerRows    = $analyzerRows
        Output          = $output
    }
}

$baseline      = Invoke-CleanBuild -UseAnalyzers:$false
$withAnalyzers = Invoke-CleanBuild -UseAnalyzers:$true

$deltaSec = $withAnalyzers.Wallclock - $baseline.Wallclock
$deltaPct = if ($baseline.Wallclock -gt 0) { ($deltaSec / $baseline.Wallclock) * 100 } else { 0 }

Write-Host ''
Write-Host '=== Build timing comparison ===' -ForegroundColor Green
Write-Host ('{0,-42} {1,8:N3} s' -f 'Wall-clock, without analyzers',    $baseline.Wallclock)
Write-Host ('{0,-42} {1,8:N3} s' -f 'Wall-clock, with analyzers',       $withAnalyzers.Wallclock)
Write-Host ('{0,-42} {1,8:N3} s  ({2:+0.0;-0.0}%)' -f 'Wall-clock delta', $deltaSec, $deltaPct)

if ($null -ne $withAnalyzers.AnalyzerSeconds) {
    Write-Host ('{0,-42} {1,8:N3} s' -f 'Reported total analyzer time', $withAnalyzers.AnalyzerSeconds)
}

$top3 = $withAnalyzers.AnalyzerRows | Select-Object -First 3
if ($top3) {
    Write-Host ''
    Write-Host 'Top analyzers by cost:' -ForegroundColor Yellow
    $top3 | ForEach-Object { Write-Host "  $_" }
}

Write-Host ''
