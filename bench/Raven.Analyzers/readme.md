# Raven.Analyzers

A standalone project for developing, testing, and benchmarking the RavenDB Roslyn analyzers
(RVN001 to RVN014) against real code. It lives under `bench/` and has its own
`Raven.Analyzers.sln`. It is **not** part of `RavenDB.sln` and is never built by CI.

## What's here

| Folder | Purpose |
|---|---|
| `Examples/RVN###_*.cs` | 14 files — one per rule — each with deliberately-broken code that fires the matching diagnostic |
| `Queries/` | 5 files of correctly-written query methods (scan bloat, no diagnostics) |
| `Indexes/` | 5 files of correctly-defined indexes (scan bloat, no diagnostics) |
| `Models/` | 5 files of plain entity POCOs (scan bloat, no diagnostics) |
| `.editorconfig` | Promotes all RVN001–RVN014 from `Info` (shipped default) to `Warning` so squiggles are unmistakable |
| `compare.ps1` | Timing comparison script — see below |

## Build and see warnings

```powershell
dotnet build bench/Raven.Analyzers -c Release
```

Expected output: 14+ `warning RVN###` lines (one or more per `Examples/` file) followed
by a `Total analyzer execution time:` table printed by `<ReportAnalyzer>`.

## Open in the IDE

Open `Raven.Analyzers.sln` in Rider or Visual Studio. Squiggles appear on
every `Examples/RVN###_*.cs` file immediately after the analyzers are built. Hover over
a squiggle to see the diagnostic message and click the help-link to open the documentation.

For RVN011 and RVN012 a code fix is available: place the caret on the squiggle and press
`Alt+Enter` (Rider) or `Ctrl+.` (Visual Studio) to apply it.

After editing analyzer source code, rebuild only the analyzer project to pick up changes:

```powershell
dotnet build src/Raven.Analyzers -c Release
```

**Note:** Roslyn's language-service worker process caches analyzer DLLs in memory.
A plain rebuild does not always flush that cache, so code-fix changes may appear
not to take effect even after the project is rebuilt. If the fix still shows stale
behaviour, restart the Roslyn worker: in Rider use
`File > Invalidate Caches…` then restart; in Visual Studio simply restart the IDE.

## Measure overhead

`compare.ps1` runs each case as several clean builds (5 by default) and reports the median,
so one slow or fast build does not skew the result. The two cases are interleaved
(without, with, without, with, ...) so a transient IO or CPU hiccup tends to hit both rather
than just one. Pass `-Runs n` to change the count.

```powershell
pwsh bench/Raven.Analyzers/compare.ps1
pwsh bench/Raven.Analyzers/compare.ps1 -Runs 3
```

Sample output:

```
Benchmarking, interleaving 5 runs per case (without, with, ...)
  run 1/5: without analyzers...
  run 1/5: with analyzers...
  ...

=== Build timing comparison (median of 5 runs) ===
Median wall-clock, without analyzers       2.847 s
Median wall-clock, with analyzers          4.213 s
Median wall-clock delta                    1.366 s  (+48.0%)
Median reported analyzer time              1.201 s

Baseline samples (s)                       2.812, 2.847, 2.901, 2.788, 2.863
With-analyzers samples (s)                 4.190, 4.213, 4.255, 4.198, 4.241

Top analyzers by cost (median run):
  0.312  26  Raven.Analyzers.Queries.QueryIndexFieldAnalyzer
  0.287  24  Raven.Analyzers.Indexes.IndexDefinitionAnalyzer
  0.198  16  Raven.Analyzers.Queries.QueryProjectionFieldAnalyzer

Result written to .../bench/Raven.Analyzers/analyzers-benchmark-result.json
  FAIL: relative overhead 48.0% (threshold 20%)
```

The "median wall-clock delta" is the overhead added to a cold build. The "median reported
analyzer time" is what the C# compiler measures as pure analyzer execution (excluding I/O and
linking). The per-run samples are shown so you can see the spread behind each median.

The script also writes `analyzers-benchmark-result.json` next to itself (git-ignored):

```json
{
  "without": 2.847,
  "with": 4.213,
  "overheadAbsolute": 1.366,
  "overheadRelative": 47.98,
  "success": false
}
```

`without` and `with` are the median wall-clock seconds, `overheadAbsolute` is their difference
in seconds, `overheadRelative` is that difference as a percentage, and `success` is `true` when
`overheadRelative` is under 20%.

To run just the baseline (no analyzer diagnostics, fastest possible compile):

```powershell
dotnet build bench/Raven.Analyzers -c Release /p:UseRavenAnalyzers=false /t:Rebuild
```

## Rule reference

| File | Fires | Title |
|---|---|---|
| `RVN001_IndexMapAssignedOutsideCtor.cs` | RVN001 | Index Map or Reduce assigned outside constructor |
| `RVN002_QueryFilteringAfterProjection.cs` | RVN002 | RavenDB query operator after projection |
| `RVN003_DoubleProjectInto.cs` | RVN003 | ProjectInto called more than once in a query chain |
| `RVN004_IndexMissingMapAssignment.cs` | RVN004 | AbstractIndexCreationTask subclass is missing a Map assignment |
| `RVN005_MultiMapIndexMissingAddMap.cs` | RVN005 | Multi-map index has no AddMap call in any constructor |
| `RVN006_MultiMapIndexSingleAddMap.cs` | RVN006 | Multi-map index uses only a single AddMap |
| `RVN007_QueryFieldNotIndexed.cs` | RVN007 | Query field not present in the index projection |
| `RVN008_QueryProjectionFieldNotRetrievable.cs` | RVN008 | Projected field not retrievable under the applied ProjectionBehavior |
| `RVN009_IndexUnsupportedMethodCall.cs` | RVN009 | Unsupported method call inside index Map/Reduce expression |
| `RVN010_QueryUnsupportedMethodCall.cs` | RVN010 | Unsupported method call inside RavenDB query expression |
| `RVN011_SubscriptionStoreOpenSession.cs` | RVN011 | Use batch.OpenSession inside a subscription Run delegate |
| `RVN012_SessionLazyBatching.cs` | RVN012 | Batch independent session operations using the lazy API |
| `RVN013_QueryUnboundedResult.cs` | RVN013 | Query result is not bounded by Take() |
| `RVN014_IndexFanOut.cs` | RVN014 | Index Map fans out over a collection |
