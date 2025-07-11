# General
- Always run a full build of the entire project at the end of a task to verify that all changes are correct and don't break anything.
- Use lower verbosity for tests by default and only enable normal or higher verbosity when absolutely necessary to avoid consuming too much context.
- Run tests in release mode to make them faster using: dotnet test --configuration Release

# Test Categorization Rules

## CRITICAL Guidelines
- Always categorize based on PRIMARY functionality being tested, not file location
- Prefer specific categories over `RavenTestCategory.Core`
- Use `Core` ONLY for truly foundational, low-level functionality
- Add `using Tests.Infrastructure;` when upgrading to [RavenFact]/[RavenTheory]

## Quick Categorization Guide

### API-Based Rules:
- **Patching operations** → `Patching`: `session.Advanced.Patch()`, `PatchByQueryOperation`
- **Query operations** → `Querying`: `session.Query<>()`, `session.Advanced.DocumentQuery<>()`
- **Index operations** → `Indexes`: `AbstractIndexCreationTask`, index creation/management
- **Basic CRUD** → `ClientApi`: `session.Store()`, `session.Load()`, `session.SaveChanges()`
- **Configuration** → `Configuration`: Database settings, name validation, server config
- **Counter operations** → `Counters`: `Increment()`, counter operations (even in patch files)
- **TimeSeries operations** → `TimeSeries`: `session.TimeSeriesFor()`, time series functionality
- **Attachments** → `Attachments`: `session.Advanced.Attachments.*`

### Voron vs Corax Distinction:
- **Voron (Storage Engine)** → `Voron`: `CompactTreeFor()`, `LookupFor()`, `OpenPostingList()`, `OpenContainer()`, `Container.Allocate/Delete`, PostingList operations, compression algorithms
- **Corax (Search Engine)** → `Corax`: `IndexWriter`, `IndexSearcher`, `TermQuery`, `TermsReader`, analyzers, tokenization, ranking, suggestions
- **Client-facing Corax** → `Corax | Querying`: `session.Query<>()` with `RavenSearchEngineMode.Corax`
- **Client-facing Corax indexing** → `Corax | Indexes`: Index behavior testing with Corax engine

### Mixed Voron/Corax Tests:
- Categorize based on PRIMARY purpose
- Count API calls to determine main focus
- Directory location is NOT reliable (FastTests/Corax may test Voron components)

### Core Category (Use Sparingly):
- Low-level data structures and algorithms
- Basic infrastructure NOT specific to any subsystem
- Cross-cutting utilities like merged enumerators

## Decision Process for AI Agents:
1. **Examine test content** - What APIs are primarily used?
2. **Identify primary purpose** - What would cause the test to fail?
3. **Choose most specific category** - Don't default to Core
4. **Validate** - Would developers find this test under this category?

## Common Mistakes to Avoid:
- Using `Core` for patching, querying, or CRUD operations
- Categorizing based on file location instead of functionality
- Combining categories unnecessarily
- Defaulting to `Core` when unsure

## Template for New Tests:
```csharp
using Tests.Infrastructure;
using Xunit;

[RavenFact(RavenTestCategory.SpecificCategory)]  // Choose most specific
public void TestName()
{
    // Test implementation
}
```

## Combined Categories:
Only use combined categories when BOTH aspects are equally critical:
- `ClientApi | Core`: When testing both client operations AND low-level behavior
- `Querying | Indexes`: When testing query behavior dependent on specific index functionality

**CRITICAL**: See test/Tests.Infrastructure/RavenTestCategory.cs for complete category definitions and examples.

## Critical Category Distinctions

**CRITICAL**: Always verify the actual `RavenTestCategory` enum before categorizing tests. Different categories serve different purposes and should not be confused.
**DO NOT** confuse these categories - they test fundamentally different features!

## Primary Functionality Rule

**Always categorize based on PRIMARY functionality being tested, not file location or secondary operations.**

### **Low-level vs High-level Operations**

**Low-level operations** ? `RavenTestCategory.Core`:
- Core data structures, fundamental algorithms, basic infrastructure
- Low-level utilities that don't belong to specific subsystems

**Client API operations** ? `RavenTestCategory.ClientApi`:
- Basic CRUD: `session.Store()`, `session.Load()`, `session.Delete()`, `session.SaveChanges()`
- Session management: `OpenSession()`, `OpenAsyncSession()`
- Document store operations: Basic client-facing functionality
- Blittable operations: `BlittableJsonReaderObject`, `JsonOperationContext`, `BlittableJsonReader`
- Serialization: Custom serializers, JSON conversion, Newtonsoft integration
- HiLo ID generation: `GenerateId`, document ID conventions
- Change tracking: `WhatChanged()`, entity tracking, eviction

### **Specific Functionality Categories**

**Counter Operations** ? `RavenTestCategory.Counters`:
- ANY increment/decrement operations, regardless of file location
- API indicators: `Increment()`, `IncrementAsync()`, counter operations
- Even if in patching files, increment operations are counter operations

**TimeSeries Operations** ? `RavenTestCategory.TimeSeries`:
- Primary category for time series functionality, even when involving indexes
- API indicators: `session.TimeSeriesFor()`, `Append()`, time series indexing
- Prefer single category over combined categories like `Indexes | TimeSeries`

**Bulk Operations** ? `RavenTestCategory.BulkInsert`:
- High-volume data insertion operations
- API indicators: `store.BulkInsert()`, bulk operations
- Exception: Serialization aspects of bulk insert ? `ClientApi`

**Query Operations**:
- `RavenTestCategory.Querying`: Basic query operations, LINQ, RQL
- `RavenTestCategory.Indexes`: When testing index creation, management, or index-specific functionality
- Combined `Querying | Indexes`: When testing query behavior that depends on specific index functionality

**Voron vs Corax distinction**: 
- Use `RavenTestCategory.Voron` for tests primarily testing storage engine components: CompactTreeFor(), LookupFor(), OpenPostingList(), OpenContainer(), Container.Allocate/Delete, PostingList operations, compression algorithms
- Use `RavenTestCategory.Corax` for tests primarily testing search engine components: IndexWriter, IndexSearcher, querying, ranking, suggestions, analyzers, tokenization

**Mixed functionality**: When tests use both Voron and Corax components, categorize based on the PRIMARY purpose:
- If testing Corax functionality that happens to use Voron for verification/setup ? Corax
- If testing Voron functionality that happens to use Corax for setup ? Voron
- If tests are using a combination of functionalities like Indexes and Counters ? Indexes | Counters | Querying
- Mixed functionality tests should be derived from context, method name and methods in the same file. 
- Count of API calls can help determine primary purpose

**Directory location is NOT reliable**: Tests in FastTests/Corax may actually test Voron components (e.g., CompactTreeTests.cs). This usually happen because of bug fixes.

**Integration tests**: Tests using GetDocumentStore() with RavenSearchEngineMode.Corax are testing Corax integration ? use Corax category. The same for Lucene

**When in doubt**: Examine the test's main assertions and what behavior is being validated

- Use appropriate categories: Core for low-level utilities, Corax for search engine components, Encryption for crypto operations, and Linux for platform-specific tests, but Linux is often a default and other more specific categories may take precedence when relevant.
- Use the Core category only when the test is focused on shared, low-level functionality (e.g., primitives from Sparrow) that is not specific to any one subsystem.
- Default to the subsystem unless the logic being tested is truly cross-cutting or foundational.
- If the test is primarily validating behavior specific to a component like Voron or Corax, then use include that category.

**CRITICAL**: Test categorization must be based on functionality being tested, not directory location or validation operations.
**CRITICAL**: When selecting which category to assign RavenFact/RavenTheory, always examine the actual functionality and usage context being tested before choosing a category - don't just default to Core.
**CRITICAL**: Usually the categories already existing in a test are supposed to be correct. More categories can be added, but never remove unless explicitely required to do a mismatched classification analysis.

# Upgrade Process
- When upgrading [Fact]/[Theory] to [RavenFact]/[RavenTheory], always analyze the test content first
- Add `using Tests.Infrastructure;` if missing
- Verify categorization matches actual test functionality before upgrading

**CRITICAL**
RavenTestCategory usage rules and patterns are documented with comprehensive comments in test/Tests.Infrastructure/RavenTestCategory.cs, including usage guidelines, combination patterns, special requirements, and examples for all categories. Analyze before starting.

## Combined Categories - When to Use vs Avoid

**Use combined categories when BOTH aspects are equally important and being tested:**

**? LEGITIMATE Combined Categories:**
- `ClientApi | Core`: When testing both client operations AND low-level behavior
  - Example: CRUD operations that test both Store/Load/Delete AND change tracking (WhatChanged)
  - Example: Tests that verify both client API behavior AND serialization/compression conventions
- `Indexes | TimeSeries`: When testing TimeSeries indexing where both aspects are critical
- `Querying | Indexes`: When testing query behavior that depends on specific index functionality

**? AVOID Combined Categories when one is clearly primary:**
- Instead of `ClientApi | Patching` ? Use `Patching` (patching is primary, client API is just the interface)
- Instead of `Indexes | TimeSeries` ? Use `TimeSeries` (when TimeSeries is the primary focus)
- Instead of `ClientApi | Counters` ? Use `Counters` (counter operations are primary)

**Decision Criteria:**
1. **Equal Weight Test**: Are both categories equally important to what's being tested?
2. **Assertion Analysis**: Do the test assertions verify behavior from both categories?
3. **Failure Impact**: If either category's functionality broke, would this test fail?

**Examples of Legitimate Combined Categories:**
- **CRUD.cs tests**: `ClientApi | Core` - Testing both basic CRUD operations AND change tracking conventions
- **Compression tests**: `ClientApi | Core` - Testing both client operations AND compression conventions
- **TimeSeries indexing**: `TimeSeries | Indexes` - When both TimeSeries functionality and indexing behavior are critical

## Context Analysis Requirements

**Before categorizing any test:**
1. Examine the test method body and surrounding context (20-30 lines)
2. Identify primary API calls and assertions
3. Determine what functionality is primarily being tested
4. Check for low-level vs high-level operations
5. Verify against actual `RavenTestCategory` enum definitions

## Common Categorization Errors to Avoid

1. **ChangesApi ? Subscriptions**: These are different features
2. **File location ? Category**: Counter tests in patch files are still counter tests
3. **Combined categories**: Prefer single primary category when possible
4. **Blittable operations**: Usually `ClientApi`, unless testing low-level core functionality
5. **Serialization tests**: Usually `ClientApi`, even in bulk insert files
6. **TimeSeries with indexes**: Primary category is `TimeSeries`

## Verification Process

**When updating test categories:**
1. Create scripts to extract context around each test
2. Verify each test one-by-one by examining surrounding context
3. Apply categorization rules based on primary functionality
4. Build and test to ensure correctness
5. Document any special cases or exceptions

## Special Cases

**Include Operations**:
- Basic includes ? `ClientApi`
- Complex includes with queries ? `Querying`

**Patching Operations**:
- Document patching ? `Patching`
- Increment operations in patch files ? `Counters`

**Index Operations**:
- General indexing ? `Indexes`
- TimeSeries indexing ? `TimeSeries`
- Counter indexing ? `Counters`

**CRUD Operations (Case Study)**:
- **CRUD.cs tests**: Use `ClientApi | Core` because they test:
  - **ClientApi aspects**: Store(), Load(), Delete(), SaveChanges(), OpenSession()
  - **Core aspects**: WhatChanged(), array change detection, null handling, compression conventions
  - **Both are equally critical**: Tests would fail if either client operations OR change tracking broke
  - **Assertions verify both**: Tests assert both successful CRUD operations AND correct change tracking behavior

# Directory-Specific Rules

## FastTests/Corax Directory Warning

**WARNING**: Many tests in FastTests/Corax directory are NOT actually testing Corax functionality despite the directory name.

**Categorization Rules for FastTests/Corax Directory**:
- **CompactTreeTests.cs**: ALL tests use Voron CompactTree ? `RavenTestCategory.Voron`
- **LookupBulkTests.cs**: Tests use Voron Lookup ? `RavenTestCategory.Voron`
- **PostingListAddRemoval.cs**: Tests use Voron PostingList ? `RavenTestCategory.Voron`
- **FacetIndexingRepro.cs**: Mixed - check each test individually:
  - Container operations ? `RavenTestCategory.Voron`
  - IndexWriter/IndexSearcher operations ? `RavenTestCategory.Corax`

## Client-Facing vs Low-Level Test Categorization

**CRITICAL**: Tests using client sessions are NOT low-level Corax tests!

**Client-facing tests** (use `RavenTestCategory.Querying` or `RavenTestCategory.Indexes`):
- Uses `session.Advanced.DocumentQuery<>()`, `session.Query<>()`, `session.Advanced.RawQuery<>()`
- Uses `GetDocumentStore()` with full RavenDB server
- Tests query translation from client API to server-side execution
- **Even if Corax-specific**: Use `RavenTestCategory.Corax | RavenTestCategory.Querying`
- **If testing index behavior**: Use `RavenTestCategory.Corax | RavenTestCategory.Indexes` or `RavenTestCategory.Querying | RavenTestCategory.Indexes`

**Low-level Corax tests** (use `RavenTestCategory.Corax` only):
- Direct usage of `IndexWriter`, `IndexSearcher`, `IndexFieldsMappingBuilder`
- Direct Corax API calls: `TermQuery`, `TermsReader`, `AllEntries()`
- No client sessions, direct storage/indexing operations
- Testing internal Corax algorithms, data structures

## API Indicators

**Voron indicators** (use `RavenTestCategory.Voron`):
- `wtx.CompactTreeFor()`, `wtx.LookupFor()`, `wtx.OpenPostingList()`, `wtx.OpenContainer()`
- `Container.Allocate()`, `Container.Delete()`
- `CompactTree.`, `PostingList.`, `Lookup<>`
- `SliceSmallSet`, compression algorithms

**Client-facing Corax indicators** (use `RavenTestCategory.Corax | RavenTestCategory.Querying`):
- `session.Advanced.DocumentQuery<>()` + `Options.ForSearchEngine(RavenSearchEngineMode.Corax)`
- `session.Query<>()` + Corax-specific features
- Testing Corax-specific query optimizations through client API
- **If testing index behavior**: Use `RavenTestCategory.Corax | RavenTestCategory.Indexes`

**Low-level Corax indicators** (use `RavenTestCategory.Corax` only):
- Direct `IndexWriter`, `IndexSearcher`, `IndexFieldsMappingBuilder` usage
- `TermQuery`, `TermsReader`, `AllEntries()`, ranking, suggestions
- No session usage, direct Corax API calls

## Mixed Tests

When tests use both Voron and Corax APIs:
1. Count primary API calls
2. Determine main purpose from test assertions
3. If testing Corax behavior that uses Voron for verification ? Corax
4. If testing Voron behavior that uses Corax for setup ? Voron

# Memory for Future Work

When working on test categorization:
- Always check this file first for guidelines
- Verify category distinctions in `RavenTestCategory` enum
- Use context analysis scripts for systematic verification
- Document any new patterns or exceptions discovered
- Update this file with new learnings

# Test Suite Considerations

## FastTests vs SlowTests

**FastTests**: Focus on speed and isolation
- Prefer unit tests and fast integration tests
- Minimize external dependencies
- Use in-memory stores when possible
- Categorization should still follow functionality-based rules

**SlowTests**: Allow for comprehensive integration testing
- Full cluster testing, replication scenarios
- Long-running operations, stress testing
- Complex multi-node scenarios
- Same categorization rules apply, but may involve more combined categories

**Both test suites** should follow the same categorization principles outlined in this document.
