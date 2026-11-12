param(
    [Parameter(Mandatory = $true)]
    [string]$Label,

    # BenchmarkDotNet filter, e.g. '*Document*'
    [string]$Filter = '*',

    # libzstd binaries to compare, as <id>=<path>; the first one is the baseline. Default: the binary in libs/libzstd.
    [string[]]$Lib = @(),

    [switch]$Quick,

    [switch]$NoReport
)

$ErrorActionPreference = 'Stop'
$repo = Resolve-Path (Join-Path $PSScriptRoot '..\..')
$artifacts = Join-Path $repo ".agents\zstd-bench\$Label"
$project = Join-Path $PSScriptRoot 'Zstd.Benchmark.csproj'
$exe = Join-Path $PSScriptRoot 'bin\Release\net10.0\Zstd.Benchmark.exe'

dotnet build $project -c Release
if ($LASTEXITCODE -ne 0) { throw 'build failed' }

New-Item -ItemType Directory -Force $artifacts | Out-Null

$runArgs = @('run', '--artifacts', $artifacts, '--filter', $Filter)
foreach ($l in $Lib) { $runArgs += @('--lib', $l) }
if ($Quick) { $runArgs += '--quick' }

& $exe @runArgs 2>&1 | Tee-Object -FilePath (Join-Path $artifacts 'run.log')
if ($LASTEXITCODE -ne 0) { throw 'benchmark run failed' }

if (-not $NoReport) {
    if ($Lib.Count -eq 0) {
        & $exe report --out (Join-Path $artifacts 'report.md') | Out-Null
    }
    foreach ($l in $Lib) {
        $id, $path = $l.Split('=', 2)
        & $exe report --lib $path --out (Join-Path $artifacts "report-$id.md") | Out-Null
    }
}

Write-Host "Results: $artifacts"
