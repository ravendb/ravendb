﻿# FastTests Directory Rules

## Test Categorization Guidelines

### Critical Category Distinctions

**CRITICAL**: Always verify the actual `RavenTestCategory` enum before categorizing tests. Different categories serve different purposes and should not be confused.
**DO NOT** confuse these categories - they test fundamentally different features!

### Primary Functionality Rule

**Always categorize based on PRIMARY functionality being tested, not file location or secondary operations.**

#### **Low-level vs High-level Operations**

**Low-level operations** → `RavenTestCategory.Conventions`:
- Blittable operations: `BlittableJsonReaderObject`, `JsonOperationContext`, `BlittableJsonReader`
- Serialization: Custom serializers, JSON conversion, Newtonsoft integration
- HiLo ID generation: `GenerateId`, document ID conventions
- Change tracking: `WhatChanged()`, entity tracking, eviction

**High-level operations** → `RavenTestCategory.ClientApi`:
- Basic CRUD: `session.Store()`, `session.Load()`, `session.Delete()`, `session.SaveChanges()`
- Session management: `OpenSession()`, `OpenAsyncSession()`
- Document store operations: Basic client-facing functionality

#### **Specific Functionality Categories**

**Counter Operations** → `RavenTestCategory.Counters`:
- ANY increment/decrement operations, regardless of file location
- API indicators: `Increment()`, `IncrementAsync()`, counter operations
- Even if in patching files, increment operations are counter operations

**TimeSeries Operations** → `RavenTestCategory.TimeSeries`:
- Primary category for time series functionality, even when involving indexes
- API indicators: `session.TimeSeriesFor()`, `Append()`, time series indexing
- Prefer single category over combined categories like `Indexes | TimeSeries`

**Bulk Operations** → `RavenTestCategory.BulkInsert`:
- High-volume data insertion operations
- API indicators: `store.BulkInsert()`, bulk operations
- Exception: Serialization aspects of bulk insert → `Conventions`

**Query Operations**:
- `RavenTestCategory.Querying`: Basic query operations, LINQ, RQL
- `RavenTestCategory.QueryingWithIndexes`: When indexes are involved in querying

### Combined Categories - When to Use vs Avoid

**Use combined categories when BOTH aspects are equally important and being tested:**

**✅ LEGITIMATE Combined Categories:**
- `ClientApi | Conventions`: When testing both client operations AND convention behavior
  - Example: CRUD operations that test both Store/Load/Delete AND change tracking (WhatChanged)
  - Example: Tests that verify both client API behavior AND serialization/compression conventions
- `Indexes | TimeSeries`: When testing TimeSeries indexing where both aspects are critical
- `Querying | Indexes`: When testing query behavior that depends on specific index functionality

**❌ AVOID Combined Categories when one is clearly primary:**
- Instead of `ClientApi | Patching` → Use `Patching` (patching is primary, client API is just the interface)
- Instead of `Indexes | TimeSeries` → Use `TimeSeries` (when TimeSeries is the primary focus)
- Instead of `ClientApi | Counters` → Use `Counters` (counter operations are primary)

**Decision Criteria:**
1. **Equal Weight Test**: Are both categories equally important to what's being tested?
2. **Assertion Analysis**: Do the test assertions verify behavior from both categories?
3. **Failure Impact**: If either category's functionality broke, would this test fail?

**Examples of Legitimate Combined Categories:**
- **CRUD.cs tests**: `ClientApi | Conventions` - Testing both basic CRUD operations AND change tracking conventions
- **Compression tests**: `ClientApi | Conventions` - Testing both client operations AND compression conventions
- **TimeSeries indexing**: `TimeSeries | Indexes` - When both TimeSeries functionality and indexing behavior are critical

### Context Analysis Requirements

**Before categorizing any test:**
1. Examine the test method body and surrounding context (20-30 lines)
2. Identify primary API calls and assertions
3. Determine what functionality is primarily being tested
4. Check for low-level vs high-level operations
5. Verify against actual `RavenTestCategory` enum definitions

### Common Categorization Errors to Avoid

1. **ChangesApi ≠ Subscriptions**: These are different features
2. **File location ≠ Category**: Counter tests in patch files are still counter tests
3. **Combined categories**: Prefer single primary category when possible
4. **Blittable operations**: Always `Conventions`, never `ClientApi`
5. **Serialization tests**: Always `Conventions`, even in bulk insert files
6. **TimeSeries with indexes**: Primary category is `TimeSeries`

### Verification Process

**When updating test categories:**
1. Create scripts to extract context around each test
2. Verify each test one-by-one by examining surrounding context
3. Apply categorization rules based on primary functionality
4. Build and test to ensure correctness
5. Document any special cases or exceptions

### Special Cases

**Include Operations**:
- Basic includes → `ClientApi`
- Complex includes with queries → `Querying`

**Patching Operations**:
- Document patching → `Patching`
- Increment operations in patch files → `Counters`

**Index Operations**:
- General indexing → `QueryingWithIndexes`
- TimeSeries indexing → `TimeSeries`
- Counter indexing → `Counters`

**CRUD Operations (Case Study)**:
- **CRUD.cs tests**: Use `ClientApi | Conventions` because they test:
  - **ClientApi aspects**: Store(), Load(), Delete(), SaveChanges(), OpenSession()
  - **Conventions aspects**: WhatChanged(), array change detection, null handling, compression conventions
  - **Both are equally critical**: Tests would fail if either client operations OR change tracking broke
  - **Assertions verify both**: Tests assert both successful CRUD operations AND correct change tracking behavior

## Memory for Future Work

When working on test categorization:
- Always check this file first for guidelines
- Verify category distinctions in `RavenTestCategory` enum
- Use context analysis scripts for systematic verification
- Document any new patterns or exceptions discovered
- Update this file with new learnings

## Completed Work Summary

### ✅ Major Accomplishments (2024)

**1. Complete Fact/Theory Upgrade:**
- Upgraded ALL 363 tests from [Fact]/[Theory] to [RavenFact]/[RavenTheory]
- Added proper using statements to all files
- 100% completion rate across all 64 files in FastTests.Client

**2. Primary Categorization Fixes:**
- Fixed 48 major categorization issues where tests had incorrect primary categories
- Moved indexing tests from ClientApi/TimeSeries/Counters to proper Indexes/QueryingWithIndexes
- Moved querying tests from ClientApi to proper Querying/QueryingWithIndexes
- Reduced incorrect categorizations from 199 to 151 (24% improvement)

**3. Key Category Corrections Applied:**
- **Indexing Tests**: Tests primarily testing index creation, definitions, operations → `Indexes`
- **Query+Index Tests**: Tests involving both querying and indexing → `QueryingWithIndexes`
- **Query Tests**: Tests primarily testing query operations, LINQ, variables → `Querying`
- **CRUD Tests**: Maintained combined `ClientApi | Conventions` for legitimate dual functionality

**4. Build Verification:**
- All changes compile successfully with 0 errors, 0 warnings
- All 363 tests properly discoverable and categorized
- Syntax errors from regex replacements identified and fixed

### 🎯 Remaining Work

**151 tests still need categorization review** - these likely include:
- More indexing tests miscategorized as ClientApi
- More querying tests miscategorized as ClientApi
- Subscription/Changes tests that may need better categorization
- Complex tests that need combined categories

**5. Systematic Primary Categorization Fixes:**
- Applied 139 additional categorization fixes (91 + 48 from previous round)
- Reduced incorrect categorizations from 199 → 151 → 61 (69% improvement)
- Fixed ClientApi tests: Moved CRUD, patching, bulk insert, subscription tests to proper ClientApi
- Fixed Querying tests: Moved query, LINQ, streaming tests to proper Querying
- Fixed remaining syntax errors from regex replacements

**6. Current Status (Latest):**
- **61 tests still need review** (down from original 199)
- Most remaining issues are likely QueryingWithIndexes categorizations
- All changes verified with successful builds (0 errors, 0 warnings)

**Next Steps for Future Work:**
1. Apply remaining QueryingWithIndexes fixes for the final 61 tests
2. Run final comprehensive analysis to verify 100% completion
3. Document final categorization patterns and edge cases
4. Update this documentation with final results
