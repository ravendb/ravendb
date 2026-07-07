# AGENTS.md

This file is the canonical guidance for AI coding agents working in this repository.

## Agent Behavior Rules

- Always run validation even for trivial doc/test changes.
- Run a full build (`dotnet build RavenDB.sln`) at the end of a task to confirm nothing broke.
- If working on FastTests, prefer speed and isolate dependencies.
- Continue tasks without stopping at checkpoints until completely finished.
- Prefer scripts over manual searching/counting to track exact locations and progress — LLMs are unreliable at counting.
- Scripts must return strings that include the filename, line number, and surrounding context so the agent can act on them.
- Agents may only use the `.agents/` directory to store partial files, scripts, and scratch output.

## Repository Overview

RavenDB is an ACID-compliant NoSQL document database. C# server and client, TypeScript/React web Studio UI.

## Prerequisites

- .NET SDK as pinned in `global.json` (`rollForward: latestFeature` lets the build resolve a compatible installed SDK; if none is present, install the version `global.json` resolves to)
- Node.js LTS (for Studio)
- PowerShell (for release builds)

## Build Commands

```bash
# .NET build (server + client) - use this for most work
dotnet build RavenDB.sln -c Release

# Studio initial setup
cd src/Raven.Studio && npm ci && npm run restore_compile

# Studio development (watch mode)
cd src/Raven.Studio && npm run webpack-watch

# Full release build (rarely needed)
./build.ps1 -LinuxX64                  # Windows
./build.sh -LinuxX64                   # Linux/Mac
./build.ps1 -JustStudio               # Studio only
./build.ps1 -LinuxX64 -DontRebuildStudio  # Skip Studio rebuild
```

## Testing

```bash
# Fast tests - primary validation, run frequently
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

Release is the default for speed. Debug builds enable assertions and `DEBUG`-conditional validation that Release strips out, so run correctness-sensitive changes (or anything you're debugging) in Debug to exercise those extra checks.

Run a category subset (categories are published as xUnit traits):

```bash
# All Querying tests in FastTests
dotnet test test/FastTests --configuration Release --filter "Category=Querying"

# Combine a category with a class/method filter
dotnet test test/FastTests --configuration Release --filter "Category=Voron&FullyQualifiedName~CompactTree"
```

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
| **FastTests** | Unit and fast integration tests - primary validation |
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

## Test Authoring & Categorization

All rules for writing and categorizing tests — base classes, `[RavenFact]`/`[RavenTheory]` (CI rejects plain `[Fact]`/`[Theory]`), and choosing the right `RavenTestCategory` — live in **[`test/AGENTS.md`](test/AGENTS.md)**. Read it before adding or recategorizing tests.

## Studio Architecture

The Studio (`src/Raven.Studio`) is a **Knockout.js → React hybrid** managed by Durandal as the SPA shell:
- **Legacy**: Knockout.js viewmodels in `typescript/viewmodels/`, paired with HTML views
- **Modern**: React components in `typescript/components/` (pages, common, hooks)
- New UI work should use React. Storybook is available (`npm run storybook`).

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

- **Custom NuGet packages**: `libs/` folder contains forked/vendored packages referenced via `NuGet.Config`. These must be present for builds to work.
- **Server dev launch settings**: Local dev runs with `--Features.Availability=Experimental` and `--Security.UnsecuredAccessAllowed=PublicNetwork` (see `src/Raven.Server/Properties/launchSettings.json`)
- **Documentation**: https://docs.ravendb.net/6.2 (this branch)
