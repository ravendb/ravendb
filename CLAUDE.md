# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

RavenDB is an ACID-compliant NoSQL document database. C# (.NET 8.0) server and client, TypeScript/React web Studio UI. Licensed under AGPLv3.

## Prerequisites

- .NET SDK 8.0.419 (exact version in `global.json`, uses `rollForward: latestFeature`)
- Node.js >= 20.0.0 LTS (for Studio)
- PowerShell (for release builds)

## Build Commands

```bash
# .NET build (server + client, ~5 min) - use this for most work
dotnet build RavenDB.sln -c Release

# Studio initial setup
cd src/Raven.Studio && npm ci && npm run restore_compile

# Studio development (watch mode)
cd src/Raven.Studio && npm run webpack-watch

# Full release build (20+ min, rarely needed)
./build.ps1 -LinuxX64                  # Windows
./build.sh -LinuxX64                   # Linux/Mac
./build.ps1 -JustStudio               # Studio only
./build.ps1 -LinuxX64 -DontRebuildStudio  # Skip Studio rebuild
```

## Testing

```bash
# Fast tests (2-5 min) - primary validation, run frequently
cd test/FastTests && dotnet test --configuration Release

# Run a single test class
dotnet test test/FastTests --configuration Release --filter "FullyQualifiedName~ClassName"

# Run a single test method
dotnet test test/FastTests --configuration Release --filter "FullyQualifiedName~ClassName.MethodName"

# Slow tests (integration, long-running)
cd test/SlowTests && dotnet test --configuration Release

# Studio tests
cd src/Raven.Studio && npm test

# Studio lint/format validation (required for CI)
cd src/Raven.Studio && npm run prettier && npm run lint
```

Always run tests in Release mode unless debugging specifically requires Debug mode.

## Running the Server

1. `dotnet build -c Release`
2. Set startup project: `src/Raven.Server`
3. Studio available at http://127.0.0.1:8080/
4. Set `RAVEN_LICENSE` env variable for dev license

## CI / PR Requirements

- **Commit messages**: Must follow format `RavenDB-#### Description` (YouTrack issue number)
- **No tabs in source files**: CI will fail on tab characters in `.cs` files
- **CLA signed**: All commit authors must sign the [CLA](https://ravendb.net/contributors/cla/sign)
- **PR template**: Use `.github/pull_request_template.md`
- **Issues tracked in**: [YouTrack](https://issues.hibernatingrhinos.com/issues/RavenDB)
- Never update package versions unless explicitly required

## Architecture

### Source (`src/`)

| Project | Purpose |
|---------|---------|
| **Raven.Server** | Core database server: documents, indexing, cluster, HTTP endpoints, Rachis consensus |
| **Raven.Client** | Public .NET client library for interacting with RavenDB servers |
| **Raven.Studio** | React/TypeScript web UI (Knockout.js legacy + React migration) |
| **Voron** | Low-level storage engine: page management, transactions, B+ trees, compression |
| **Corax** | Search/indexing engine: inverted indexes, tokenization, analyzers, ranking |
| **Sparrow** | Low-level system utilities: memory, compression, hashing (no RavenDB logic) |
| **Sparrow.Server** | Server-only utilities built on Sparrow |
| **Raven.Pal** | Platform Abstraction Layer: OS-specific I/O, memory mapping, signals |
| **Raven.Embedded** | Self-contained embedded RavenDB deployment |
| **Raven.TestDriver** | Test harness for integration/system tests |

### Tests (`test/`)

| Project | Purpose |
|---------|---------|
| **FastTests** | Unit and fast integration tests (2-5 min) - primary validation |
| **SlowTests** | Long-running integration, cluster, concurrency tests |
| **Tests.Infrastructure** | Shared test scaffolding, `RavenTestCategory`, base classes |
| **RachisTests** | Consensus protocol and cluster behavior |
| **StressTests** | Performance and memory pressure |
| **InterversionTests** | Cross-version compatibility |
| **EmbeddedTests** | Embedded mode tests |
| **Tryouts** | Scratch/experimental code |

### Key Server Subsystems (`src/Raven.Server/`)

- **Documents/** - Document storage, CRUD, attachments, conflicts, counters, time series
- **Indexing/** - Index management, map-reduce, auto-indexes
- **Rachis/** - Raft consensus protocol implementation for cluster
- **Web/** - HTTP request handlers and routing
- **ServerWide/** - Cross-database operations, cluster management
- **Background/** - Background tasks and operations
- **Smuggler/** - Import/export functionality

## Test Rules

**Every new test must:**
1. Inherit from `RavenTestBase` (most common), `ClusterTestBase`, `ReplicationTestBase`, or another appropriate base class from `Tests.Infrastructure`. CI enforces that test classes implement `IDisposable` (which the base classes provide).
2. Use `[RavenFact(RavenTestCategory.X)]` / `[RavenTheory(RavenTestCategory.X)]` instead of plain `[Fact]`/`[Theory]`. CI will fail if standard xUnit attributes are used. Add `using Tests.Infrastructure;`. Categories can be combined with `|` for tests spanning multiple areas.

```csharp
// Single category
[RavenFact(RavenTestCategory.Querying)]
public void MyQueryTest() { ... }

// Combined categories
[RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
public async Task MyClusterShardingTest() { ... }

// Parameterized test
[RavenTheory(RavenTestCategory.Etl)]
[InlineData(RavenDatabaseMode.Single, RavenDatabaseMode.Sharded)]
public void MyEtlTest(RavenDatabaseMode src, RavenDatabaseMode dst) { ... }
```

### Running tests by category

Categories are published as xUnit traits. Filter with `--filter`:

```bash
# Run only Querying tests in FastTests
dotnet test test/FastTests --configuration Release --filter "Category=Querying"

# Run only Cluster tests in SlowTests
dotnet test test/SlowTests --configuration Release --filter "Category=Cluster"

# Combine with class/method filters
dotnet test test/FastTests --configuration Release --filter "Category=Voron&FullyQualifiedName~CompactTree"
```

### Categorization rules

- Categorize based on PRIMARY functionality being tested, not file/directory location
- Read `test/Tests.Infrastructure/RavenTestCategory.cs` for complete category definitions (54+ categories with detailed comments)
- Prefer specific categories over `RavenTestCategory.Core`
- After changing test attributes, update the count in `SlowTests.Tests.TestsInheritanceTests.AllTestsShouldUseRavenFactOrRavenTheoryAttributes`
- Attributes also support `LicenseRequired`, `NightlyBuildRequired`, and `Requires` (for external services like MsSql, ElasticSearch) which auto-skip tests when prerequisites are missing

### Quick categorization guide

- `session.Query<>()` / `session.Advanced.DocumentQuery<>()` -> `Querying`
- `session.Advanced.Patch()` / `PatchByQueryOperation` -> `Patching`
- `session.Store()` / `session.Load()` basic CRUD -> `ClientApi`
- `AbstractIndexCreationTask`, index management -> `Indexes`
- `session.TimeSeriesFor()` -> `TimeSeries`
- `session.Advanced.Attachments.*` -> `Attachments`
- `Increment()`, counter operations -> `Counters`
- Direct `CompactTreeFor()`, `LookupFor()`, `OpenPostingList()` -> `Voron`
- Direct `IndexWriter`, `IndexSearcher`, `TermQuery` -> `Corax`
- Client-facing with `RavenSearchEngineMode.Corax` -> `Corax | Querying`

**Search engine-specific tests**: Set the search engine directly in `GetDocumentStore()` call:
```csharp
GetDocumentStore(new Options { SearchEngine = RavenSearchEngineMode.Lucene })
```

## Studio Architecture

The Studio (`src/Raven.Studio`) is a **Knockout.js → React hybrid** managed by Durandal as the SPA shell:
- **Legacy**: Knockout.js viewmodels in `typescript/viewmodels/`, paired with HTML views
- **Modern**: React components in `typescript/components/` (pages, common, hooks), using Redux Toolkit, React Hook Form, TanStack Table
- New UI work should use React. Storybook is available (`npm run storybook`, port 6006).

## Code Conventions

### C# style (enforced by `.editorconfig`)
- Private/internal fields: `_camelCase` prefix
- Newlines before all braces
- Explicit types preferred (avoid `var`)
- C# preview language features enabled (`LangVersion: preview`)
- Warnings treated as errors

### General
- Code marked as `PERF` is performance-sensitive - do not modify unless explicitly required; notify the user if changes are needed
- Remove copyright notices from files (project no longer uses them)
- Remove UTF-8 BOM markers when found
- Server HTTP handlers must inherit from `ServerRequestHandler`, `DatabaseRequestHandler`, or `ShardedDatabaseRequestHandler` (not `RequestHandler` directly) - CI enforces this

## Other Notes

- **Custom NuGet packages**: `libs/` folder contains forked packages (Jint, Lucene.NET, Microsoft.Diagnostics.Runtime) referenced via `NuGet.Config`. These must be present for builds to work.
- **Server dev launch settings**: Local dev runs with `--Features.Availability=Experimental` and `--Security.UnsecuredAccessAllowed=PublicNetwork` (see `src/Raven.Server/Properties/launchSettings.json`)
- **Documentation**: https://docs.ravendb.net/6.2 (this branch). Latest version docs: https://docs.ravendb.net/7.2
