# Raven.Analyzers.Playground

A standalone sample project for developing and testing the RavenDB Roslyn analyzers
(RVN001–RVN014) against real code. It is **not** part of `RavenDB.sln` and is never
built by CI.

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
dotnet build samples/Raven.Analyzers.Playground -c Release
```

Expected output: 14+ `warning RVN###` lines (one or more per `Examples/` file) followed
by a `Total analyzer execution time:` table printed by `<ReportAnalyzer>`.

## Open in the IDE

Open `Raven.Analyzers.Playground.sln` in Rider or Visual Studio. Squiggles appear on
every `Examples/RVN###_*.cs` file immediately after the analyzers are built. Hover over
a squiggle to see the diagnostic message and click the help-link to open the documentation.

For RVN011 and RVN012 a code fix is available: place the caret on the squiggle and press
`Alt+Enter` (Rider) or `Ctrl+.` (Visual Studio) to apply it.

After editing analyzer source code, rebuild only the analyzer project to pick up changes:

```powershell
dotnet build src/Raven.Analyzers -c Release
```

The IDE reloads the analyzer DLL automatically when the project is rebuilt.

## Measure overhead

`compare.ps1` runs two clean builds back to back — one without analyzers (baseline) and
one with — and prints a side-by-side timing report.

```powershell
pwsh samples/Raven.Analyzers.Playground/compare.ps1
```

Sample output:

```
Building WITHOUT analyzers (baseline)...
Building WITH analyzers...

=== Build timing comparison ===
Wall-clock, without analyzers              2.847 s
Wall-clock, with analyzers                 4.213 s
Wall-clock delta                           1.366 s  (+48.0%)
Reported total analyzer time               1.201 s

Top analyzers by cost:
  0.312  26  Raven.Analyzers.Queries.QueryIndexFieldAnalyzer
  0.287  24  Raven.Analyzers.Indexes.IndexDefinitionAnalyzer
  0.198  16  Raven.Analyzers.Queries.QueryProjectionFieldAnalyzer
```

The "wall-clock delta" is the overhead added to a cold build. The "Reported total analyzer
time" is what the C# compiler measures as pure analyzer execution (excluding I/O and linking).

To run just the baseline (no analyzer diagnostics, fastest possible compile):

```powershell
dotnet build samples/Raven.Analyzers.Playground -c Release /p:UseRavenAnalyzers=false /t:Rebuild
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
