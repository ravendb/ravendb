﻿# FastTests/Corax Directory Rules

## Critical Categorization Warning
**WARNING**: Many tests in this directory are NOT actually testing Corax functionality despite the directory name.

## Categorization Rules for This Directory
- **CompactTreeTests.cs**: ALL tests use Voron CompactTree → `RavenTestCategory.Voron`
- **LookupBulkTests.cs**: Tests use Voron Lookup → `RavenTestCategory.Voron`
- **PostingListAddRemoval.cs**: Tests use Voron PostingList → `RavenTestCategory.Voron`
- **FacetIndexingRepro.cs**: Mixed - check each test individually:
  - Container operations → `RavenTestCategory.Voron`
  - IndexWriter/IndexSearcher operations → `RavenTestCategory.Corax`

## Client-Facing vs Low-Level Test Categorization
**CRITICAL**: Tests using client sessions are NOT low-level Corax tests!

**Client-facing tests** (use `RavenTestCategory.QueryingWithIndexes` or `RavenTestCategory.Querying`):
- Uses `session.Advanced.DocumentQuery<>()`, `session.Query<>()`, `session.Advanced.RawQuery<>()`
- Uses `GetDocumentStore()` with full RavenDB server
- Tests query translation from client API to server-side execution
- **Even if Corax-specific**: Use `RavenTestCategory.Corax | RavenTestCategory.QueryingWithIndexes`

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

**Client-facing Corax indicators** (use `RavenTestCategory.Corax | RavenTestCategory.QueryingWithIndexes`):
- `session.Advanced.DocumentQuery<>()` + `Options.ForSearchEngine(RavenSearchEngineMode.Corax)`
- `session.Query<>()` + Corax-specific features
- Testing Corax-specific query optimizations through client API

**Low-level Corax indicators** (use `RavenTestCategory.Corax` only):
- Direct `IndexWriter`, `IndexSearcher`, `IndexFieldsMappingBuilder` usage
- `TermQuery`, `TermsReader`, `AllEntries()`, ranking, suggestions
- No session usage, direct Corax API calls

## Mixed Tests
When tests use both Voron and Corax APIs:
1. Count primary API calls
2. Determine main purpose from test assertions
3. If testing Corax behavior that uses Voron for verification → Corax
4. If testing Voron behavior that uses Corax for setup → Voron
