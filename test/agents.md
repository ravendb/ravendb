# General
- Always run a full build of the entire project at the end of a task to verify that all changes are correct and don't break anything.
- Use lower verbosity for tests by default and only enable normal or higher verbosity when absolutely necessary to avoid consuming too much context.
- Run tests in release mode to make them faster using: dotnet test --configuration Release

# Test Categorization Rules

- **CRITICAL**: Test categorization must be based on functionality being tested, not directory location or validation operations.
- **CRITICAL**: When selecting which category to assign RavenFact/RavenTheory, always examine the actual functionality and usage context being tested before choosing a category - don't just default to Core.
- **CRITICAL**: Usually the categories already existing in a test are supposed to be correct. More categories can be added, but never remove unless explicitely required to do a mismatched classification analysis.
- **Voron vs Corax distinction**: 
  - Use `RavenTestCategory.Voron` for tests primarily testing storage engine components: CompactTreeFor(), LookupFor(), OpenPostingList(), OpenContainer(), Container.Allocate/Delete, PostingList operations, compression algorithms
  - Use `RavenTestCategory.Corax` for tests primarily testing search engine components: IndexWriter, IndexSearcher, querying, ranking, suggestions, analyzers, tokenization
- **Mixed functionality**: When tests use both Voron and Corax components, categorize based on the PRIMARY purpose:
  - If testing Corax functionality that happens to use Voron for verification/setup → Corax
  - If testing Voron functionality that happens to use Corax for setup → Voron
  - If tests are using a combination of functionalities like Indexes and Counters → Indexes | Counters | Querying
  - Mixed functionality tests should be derived from context, method name and methods in the same file. 
  - Count of API calls can help determine primary purpose
- **Directory location is NOT reliable**: Tests in FastTests/Corax may actually test Voron components (e.g., CompactTreeTests.cs). This usually happen because of bug fixes. 
- **Integration tests**: Tests using GetDocumentStore() with RavenSearchEngineMode.Corax are testing Corax integration → use Corax category. The same for Lucene
- **When in doubt**: Examine the test's main assertions and what behavior is being validated

- Use appropriate categories: Core for low-level utilities, Corax for search engine components, Encryption for crypto operations, and Linux for platform-specific tests, but Linux is often a default and other more specific categories may take precedence when relevant.
- Use the Core category only when the test is focused on shared, low-level functionality (e.g., primitives from Sparrow) that is not specific to any one subsystem.
- Default to the subsystem unless the logic being tested is truly cross-cutting or foundational.
- If the test is primarily validating behavior specific to a component like Voron or Corax, then use include that category.

# Upgrade Process
- When upgrading [Fact]/[Theory] to [RavenFact]/[RavenTheory], always analyze the test content first
- Add `using Tests.Infrastructure;` if missing
- Verify categorization matches actual test functionality before upgrading

**CRITICAL** 
RavenTestCategory usage rules and patterns are documented with comprehensive comments in test/Tests.Infrastructure/RavenTestCategory.cs, including usage guidelines, combination patterns, special requirements, and examples for all categories. Analyze before starting.