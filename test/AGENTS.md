# Test Authoring & Categorization

Rules for writing and categorizing tests. For build/run commands and source-wide conventions, see the root `AGENTS.md`.

- Use lower verbosity for tests by default; raise it only when necessary to avoid consuming too much context.

## Authoring Requirements

- Inherit from `RavenTestBase` (most common), `ClusterTestBase`, `ReplicationTestBase`, or another appropriate base class from `Tests.Infrastructure`. CI enforces that test classes implement `IDisposable` (the base classes provide it).
- Use `[RavenFact(RavenTestCategory.X)]` / `[RavenTheory(RavenTestCategory.X)]` instead of plain `[Fact]`/`[Theory]` — CI fails on standard xUnit attributes. Add `using Tests.Infrastructure;`.
- Combine categories with `|` only when more than one area is genuinely exercised.
- Attributes also support `LicenseRequired`, `NightlyBuildRequired`, and `Requires` (external services like MsSql, ElasticSearch); these auto-skip when prerequisites are missing.
- `RavenTestCategory.cs` (`test/Tests.Infrastructure/RavenTestCategory.cs`) is the authoritative source — it documents every category, combination pattern, special requirement, and example. Read it before categorizing.

```csharp
using Tests.Infrastructure;

[RavenFact(RavenTestCategory.Querying)]                                   // single
public void MyQueryTest() { ... }

[RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]       // combined
public async Task MyClusterShardingTest() { ... }

[RavenTheory(RavenTestCategory.Etl)]                                      // parameterized
[InlineData(RavenDatabaseMode.Single, RavenDatabaseMode.Sharded)]
public void MyEtlTest(RavenDatabaseMode src, RavenDatabaseMode dst) { ... }
```

## Core Principle

Categorize by the PRIMARY functionality being tested — what would cause the test to fail — not by file/directory location or by secondary setup/validation operations. Read the test body and assertions to decide, and verify the choice against the `RavenTestCategory` enum. Prefer the most specific category; reserve `Core` for truly foundational, low-level functionality not tied to any subsystem (e.g. Sparrow primitives, cross-cutting utilities). Don't default to `Core` when unsure.

Existing categories on a test are usually correct: you may add a category, but don't remove one unless explicitly doing a mismatch-analysis pass.

## API → Category

- `session.Query<>()`, `session.Advanced.DocumentQuery<>()`, `RawQuery<>()`, RQL/LINQ → `Querying`
- `session.Advanced.Patch()`, `PatchByQueryOperation` → `Patching`
- `session.Store()`, `Load()`, `Delete()`, `SaveChanges()`, `OpenSession()` → `ClientApi` (also blittable ops, custom serialization, HiLo id generation, change tracking / `WhatChanged()`)
- `AbstractIndexCreationTask`, index creation/management → `Indexes`
- `session.TimeSeriesFor()`, `Append()` → `TimeSeries`
- `session.Advanced.Attachments.*` → `Attachments`
- `Increment()` / `IncrementAsync()`, counter operations (even inside patch files) → `Counters`
- `store.BulkInsert()` → `BulkInsert` (but serialization aspects of bulk insert → `ClientApi`)
- Database settings, name validation, server config → `Configuration`

## Voron vs Corax

- **Voron** (storage engine) → `Voron`: `CompactTreeFor()`, `LookupFor()`, `OpenPostingList()`, `OpenContainer()`, `Container.Allocate/Delete`, `PostingList`/`Lookup<>` operations, compression algorithms, `SliceSmallSet`.
- **Corax** (search engine) → `Corax`: `IndexWriter`, `IndexSearcher`, `IndexFieldsMappingBuilder`, `TermQuery`, `TermsReader`, `AllEntries()`, analyzers, tokenization, ranking, suggestions.
- **Client sessions are NOT low-level Corax tests.** A test using `GetDocumentStore()` and sessions tests query translation, not the engine internals:
  - session query with `RavenSearchEngineMode.Corax` → `Corax | Querying`
  - index behavior with the Corax engine → `Corax | Indexes`
  - same pattern applies to Lucene via the engine set in `GetDocumentStore()`.
- **Directory location is NOT reliable.** Tests under `FastTests/Corax` may actually exercise Voron (a side effect of bug-fix history) — categorize by the APIs a test uses, not the folder it lives in.
- **Mixed Voron/Corax**: categorize by primary purpose — count primary API calls and read the assertions. Corax behavior that uses Voron only for setup/verification → `Corax`; the reverse → `Voron`.

## Combined Categories

Use a combination only when both aspects are equally critical, the assertions verify both, and the test would fail if either broke. Otherwise use the single primary category.

- Legitimate: `ClientApi | Core` (CRUD plus change-tracking/compression conventions), `Querying | Indexes` (query behavior dependent on a specific index), `TimeSeries | Indexes` (time-series indexing where both matter).
- Avoid: `ClientApi | Patching` → `Patching`; `Indexes | TimeSeries` → `TimeSeries`; `ClientApi | Counters` → `Counters`.

## Special Cases

- Includes: basic → `ClientApi`; complex includes with queries → `Querying`.
- Patching: document patch → `Patching`; increment inside a patch file → `Counters`.
- Indexing: general → `Indexes`; time-series indexing → `TimeSeries`; counter indexing → `Counters`.
- `ChangesApi` and `Subscriptions` are different features — do not conflate them.
- Crypto → `Encryption`. Platform-specific → `Linux`, but `Linux` is often a default and a more specific category usually takes precedence.
