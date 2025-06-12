# General
- Agents are ONLY allowed to use .agents directory to store partial files, scripts, etc.

# Test Categorization Scripts
- When creating categorization verification scripts, use intelligent logic to determine PRIMARY vs SECONDARY functionality
- Scripts should count API usage patterns to determine primary purpose (e.g., more IndexWriter calls = Corax test)
- Include specific indicators for common misclassification patterns:
  - CompactTreeFor, LookupFor, OpenPostingList, Container operations = Voron
  - IndexWriter, IndexSearcher, TermQuery, analyzers = Corax
  - GetDocumentStore with RavenSearchEngineMode.Corax = Corax integration
  - Advanced.Increment can be misinterpreted to be Counters when in fact it is Patching.
- Verification scripts should flag tests that use both Voron and Corax APIs for manual review
- Always provide file path, method name, and specific indicators found for agent action
