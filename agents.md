# Agent Behavior Rules
- Always run validation even for trivial doc/test changes.
- If working on FastTests, prefer speed, isolate dependencies.
- Prefer PowerShell script approach for tracking exact locations and progress, as it's much more efficient than manual searching
- When writing scripts ensure that scripts provide the agent filename, line number and context to act. 
- Code marked as PERF is very sensitive, do not modify unless explicitely required to. Always explain the behavior in the code and the user must be notified of that fact to ensure proper behavior.
- Continue tasks without stopping at checkpoints until completely finished.
- Prefer scripts to counting, LLMs are not good at it.
- Remove UTF8 BOM markers when found.
- Scripts must always return strings and provide the line number in order to be used by the agent to find the context.
- Copyright notices should be removed from files as the project no longer uses them.
- Rely on the context engine as much as you can.

# General Knowledge of the solution.

- build.ps1 is a release build scripting, do not use unless you need to work on it.
- All agents must ensure their changes build successfully via: dotnet build RavenDB.sln

- $root/src/ folder contains the RavenDB main source.
    - Sparrow:	Low-level system utilities: memory, compression, hashing. No RavenDB logic here.
    - Sparrow.Server: Utility logic used only by the server, built on top of Sparrow.
    - Voron: Storage engine: page management, transactions, trees, compression.
    - Corax: Search engine: indexing pipeline, tokenization, analyzers, inverted indexes.
    - Raven.Pal: Platform Abstraction Layer — OS-specific logic (memory map, I/O, signals).
    - Raven.Client: Public .NET client interface for interacting with RavenDB servers.
    - Raven.Server: Core RavenDB server-side logic (document database, cluster, HTTP endpoints).
    - Raven.Embedded: Self-contained RavenDB deployment logic.
    - Raven.Studio: Frontend web UI. Mostly TypeScript and static assets.
    - Raven.TestDriver: Simplified harness for writing integration/system tests.

- $root/test/ folder contains all test projects.
    - Tests.Infrastructure: Shared scaffolding across test projects.    
    - FastTests: Unit and fast-running integration tests.
    - SlowTests: Long-running or concurrency/cluster integration tests.
    - RachisTests: Consensus protocol and cluster-wide behavior.
    - StressTests: Performance and memory pressure tests.
    - BenchmarkTests: BenchmarkDotNet-based performance profiling.    
    - ConcurrencyTests: Race conditions and threading correctness.
    - EmbeddedTests: Tests targeting embedded RavenDB behavior.
    - LicenseTests:	Licensing and activation flows.
    - InterversionTests: Cross-version compatibility and upgrade checks.
    - Tryouts: Scratch or experimental code. Don't depend on stability.

- Tests should be run in Release mode unless the changes are required to run in debug mode: dotnet test --configuration Release
- After changing test attributes ([Fact] → [RavenFact]), update count in SlowTests.Tests.TestsInheritanceTests.AllTestsShouldUseRavenFactOrRavenTheoryAttributes
- When writing tests always use RavenFact / RavenTheory with proper category flags.
- **CRITICAL**: Read RavenTestCategory.cs for instructions on how to categorize properly.
- **CRITICAL**: Test categorization must be based on functionality being tested, not directory location.
- **CRITICAL**: Multiple categorizations may apply, consider human annotated ones (the ones committed as examples of correct categorizations, they are mostly correct).
- **CRITICAL**: Tests using client sessions (`session.Advanced.DocumentQuery<>()`, `session.Query<>()`) are client-facing tests and should use `RavenTestCategory.Querying`, NOT low-level engine categories like `RavenTestCategory.Corax` or `RavenTestCategory.Voron`.
- **Exception**: If a test is engine-specific (e.g., uses `Options.ForSearchEngine(RavenSearchEngineMode.Corax)`), use combined categories like `RavenTestCategory.Corax | RavenTestCategory.Querying`.
- **Search Engine Specific Tests**: For tests that use engine-specific features (e.g., Lucene analyzers, WhereLucene), set the search engine directly in GetDocumentStore() call instead of using [RavenData] attributes: `GetDocumentStore(new Options { SearchEngine = RavenSearchEngineMode.Lucene })`
- **Index-related tests**: Use `RavenTestCategory.Indexes` for index creation/management, or `RavenTestCategory.Querying | RavenTestCategory.Indexes` for query behavior that depends on specific index functionality.
- When upgrading [Fact]/[Theory] attributes, always analyze test content to determine correct categorization before upgrading.
- Always run the fast tests to ensure changes do work.
