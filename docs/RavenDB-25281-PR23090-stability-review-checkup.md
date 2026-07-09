# PR #23090 stability review — per-finding checkup against v7.2 (old Corax)

**Branch:** `RavenDB-25281-corax-72-fixes` (v7.2 line, **old Corax 1.x** — not the Corax 2.0 rewrite)
**Source review:** ravendb/ravendb#23090 review `4639510521` ("Stability review — Corax 2.0, automated multi-agent", 28 findings: 8 🔴 / 20 🟡, self-labeled *unvalidated candidates*).
**Method:** per-file existence sweep on this branch, then four parallel read-only investigations that traced the actual code (line numbers in the review are diff-offsets, not file lines).

## Headline

The review targets the **Corax 2.0 rewrite** on `v8.0`. This branch runs **old Corax** and lacks the 2.0 engine (`Planning/`, `RoaringBitmaps/`, `QueryPlanBuilder/`, `TermsProviders/`, `BitmapMatch`, `CompiledQuery`, IL residual-scan, …).

- **20 of 28 findings are N/A to v7.2** — the flagged file or the flagged code pattern/type does not exist on this branch.
- **8 findings touch code that does exist here** — and **every one is benign, transaction-bounded, or a deliberate documented tradeoff. None is a correctness bug or an unbounded leak.**
- **Actionable defects on v7.2: 0.** One item (F-VS1) is a genuine but transaction-bounded inefficiency worth a low-priority tidy-up; the rest need no change.

---

## Part 1 — findings whose code exists on this branch (investigated in depth)

### F-VS1 — abandoned `Hnsw.SearchState` in the filtered-vector-search seed enumerator
- **Review:** `IndexSearcher.VectorSearch.cs:85` — SearchState (IDisposable, holds `NativeList<Node>`) created and never disposed → native leak. (🔴)
- **v7.2 applicable:** **Yes.** `RandomNodesFromFilterEnumerator` ctor (`IndexSearcher.VectorSearch.cs:74-92`) and the sibling `TryConvertDocumentsIdsToNodesIds` (`:179-186`) both do `var searchState = new Hnsw.SearchState(...)` as a **local**, extract `NodeIdsByVectorId`, and never store or dispose it. `Hnsw.SearchState` (`Hnsw.cs:118`) is `IDisposable` with **no finalizer**.
- **Verdict:** **Real defect, but NOT an unbounded native leak.** All of SearchState's native buffers (`_nodes`, `_newNodes`, nested edge lists) are allocated through `Llt.Allocator` — the transaction's `ByteStringContext` arena, which bulk-frees every segment at `LowLevelTransaction.Dispose()` regardless of per-object `Release()`. So the memory is reclaimed at transaction end; what's lost is intra-transaction *reuse* (dead space accumulates across N filtered-vector searches in one long transaction) plus a small managed object. It also bypasses the codebase's own established contract — every other `SearchState` goes through `IndexSearcher.GetOrCreateVectorSearchState()` (`IndexSearcher.cs:494`), which caches per field and disposes in `IndexSearcher.Dispose()`.
- **Fix (documented, not applied):** route both sites through `indexSearcher.GetOrCreateVectorSearchState(metadata.FieldName)` instead of `new Hnsw.SearchState(...)` — matches what `VectorSearchMatch.InitializeVectorSearch()` already does for the retriever, removes the abandoned instance, and dedups the state per field. (Alternative: store the local in a field and dispose it in `Dispose()`.) *Caveat before applying:* confirm sharing the cached state's `NodeIdsByVectorId` with the retriever causes no priority-queue interference — the enumerator only reads the lookup, so it should be safe, but it touches HNSW internals and warrants a filtered-vector-search test.
- **Verification:** static — grep confirms `searchState` at `:81`/`:181` is never fielded/disposed; dynamic — repeatedly issue `ApproximateFilteredNearest` (isExact=false + filterQuery) in one long transaction and watch `ByteStringContext._wholeSegments` grow 1:1 with call count, then flatten after the fix.
- **Severity:** bounded leak / contract-bypass code smell. **Confidence:** high.

### F-SR1 — `IndexSearcher.Search` methods lack try/finally (temp allocations on exception)
- **Review:** `IndexSearcher.Search.cs:42` and `:96` — search accumulation / `AccumulatePhraseQuery` leak `BitmapMatch`/`RoaringBitmap` temporaries on exception. (🔴 + 🟡)
- **v7.2 applicable:** **Partially.** The *files/methods* exist (`SearchQueryLegacy` `:43`, `SearchQueryWithPhraseQuery` `:189`, `…WildcardQueriesAdjustments` `:372`), but the `BitmapMatch`/`RoaringBitmap` premise does **not** — old Corax has neither.
- **Verdict:** **Not a leak.** The real temporaries (`ContextBoundNativeList<Slice>`, `Analyzer`) are all `Allocator`-scoped (= `_transaction.Allocator`) and bulk-freed at transaction dispose; `IQueryMatch` has no `Dispose()` at all in old Corax (the whole match graph is arena-scoped by design). `Analyzer.Dispose()` is effectively empty (thread-static pools rented/returned within the call). A mid-loop throw leaks nothing beyond a bounded delay to the guaranteed tx teardown.
- **Fix:** none needed. Optional cleanliness: wrap loops in try/finally to dispose `terms`/`wildcardAnalyzer` promptly. **Verification:** traced `Allocator ⇒ _transaction.Allocator` (`IndexSearcher.cs:94`), `LowLevelTransaction.Dispose ⇒ _allocator.Dispose()` (`:1015`), `ByteStringContext.Dispose` bulk-frees segments (`ByteString.cs:1982`). **Severity:** benign. **Confidence:** high.

### F-PM1 — `PhraseMatch<TInner>` stores `TInner` by value without cascading `Dispose`
- **Review:** `PharseMatch.cs:15` — `PharseMatch<BitmapMatch>` copies a disposable struct by value, no `Dispose` cascade. (🟡)
- **v7.2 applicable:** **No (file present, premise N/A).** `PhraseMatch<TInner>` (`PharseMatch.cs:15`) exists but is **not** `IDisposable`, and **no `IQueryMatch` implementor in old Corax is `IDisposable`** (grep of `src/Corax/Querying/Matches/` finds none). At the only call sites (`IndexSearcher.Search.cs:298,453`) `TInner` is inferred as the interface `IQueryMatch` (from `AllInQuery`'s declared return type) — a boxed reference, never a native-owning disposable struct. The v8 `BitmapMatch` doesn't exist here.
- **Verdict:** non-issue. **Fix:** none — becomes relevant only if/when a disposable match struct is introduced (i.e., on a 2.0 rebase). **Verification:** grep `: IDisposable` in Matches/ (only unrelated `SuggestionsNGramTable`); `IQueryMatch` interface has no `Dispose`. **Confidence:** high.

### F-RX1 / F-RX2 — `ConcurrentLruRegexCache` `ThreadLocal<Regex>` not disposed (race-loser / eviction)
- **Review:** `ConcurrentLruRegexCache.cs:70` (contention) & `:104` (eviction) — undisposed `ThreadLocal<Regex>`. (🟡 ×2)
- **v7.2 applicable:** **Yes.** This branch's commit `2b40db6f0a4` switched `Lazy<Regex>` → `ThreadLocal<Regex>`. Node (`:120-136`) is **not** `IDisposable`; neither the `GetOrAdd` race-loss (`:69`) nor eviction (`TryRemove`, `:105`) disposes it.
- **Verdict:** **Mechanically real, but bounded and deliberate — not an unbounded leak.** `ThreadLocal<T>` is `IDisposable` *and* finalizable, so undisposed instances are GC-reclaimed. Cache is capacity-bounded (1024/instance; evicts ≤256/pass). On the race path the loser's `ThreadLocal` is essentially empty (its `.Value` is never realized before discard). Both gaps are called out by **author comments added in the same commit** (`:67-68`, `:96-98`) — a conscious tradeoff. F-RX2's real cost is delayed managed reclamation (per-thread `Regex`, more so with `RegexOptions.Compiled`) scaling with threads×eviction, not a handle leak.
- **Fix (documented, not applied):** make the node `IDisposable`, dispose the race-loser (safe — never published) and evicted nodes. **Do not apply naively:** disposing on eviction races with a concurrent reader that grabbed the node via `TryGetValue` just before removal → `ObjectDisposedException` on `.Value` (a regression that doesn't exist today). A safe fix needs deferred/quiescent disposal, so this is a design decision, not a drop-in.
- **Verification:** capacity-bounded eviction test with a per-thread compile counter + finalizer hook; plus a concurrency stress test gating any immediate-dispose attempt against `ObjectDisposedException`. **Severity:** bounded/delayed reclamation. **Confidence:** high (facts), medium (naive-fix safety).

### F-CK1 / F-CK2 / F-CK3 — `CompactKey` cross-pool `ArrayPool` return
- **Review:** `CompactKey.cs:276/133/136` — arrays rented on one core's per-core `ArrayPool` returned to another's when the thread migrates. (🟡 ×3)
- **v7.2 applicable:** **Yes.** `GetPoolFrom<T>` (`CompactKey.cs:31`) re-derives the pool from `Thread.GetCurrentProcessorId()` at *every* Rent/Return; `Initialize` (`:84-85`) rents, `Reset` (`:95,98`) and `UnlikelyGrowStorage` (`:242,245`) return — each an independent core read, so a scheduler-driven core change between them mismatches the pools. All three findings share this one root cause (F-CK1 grow-path `_storage`, F-CK2 reset-path `_storage`, F-CK3 `_keyMappingCache`).
- **Verdict:** **Benign perf imbalance — not a leak, not corruption.** `ArrayPool<T>.Create()` performs **no origin check**; a "wrong-pool" return is accepted and re-rentable, all pools are configured identically, and a full bucket just drops to GC exactly as the "right" pool would. Net effect is statistical cache-population skew (self-healing random walk), never a lost or unreturnable array.
- **Fix (documented, not applied):** if tightening is wanted, cache the pool reference once per instance in `Initialize` (`_storagePool`/`_keyMappingPool`) and reuse it in Reset/Grow — exactly the property the older reverted `PerCoreStatic` design had. **Not recommended as a hotfix:** this is `PERF`-sensitive pooling code with recent intentional churn by the author; flag rather than unilaterally change.
- **Verification:** not functionally testable (no incorrect output/throw/leak); premise confirmed from `Thread.GetCurrentProcessorId()` semantics (may change between calls) + `ArrayPool.Create()` having no owner tracking. **Severity:** benign-perf. **Confidence:** high.

### F-FAC1 / F-CMP1 / F-IVT1 / F-TST1 — file exists, but the flagged pattern is Corax 2.0-only
- **F-FAC1** (`CoraxIndexFacetedReadOperation.cs`, base-query try/finally): **N/A.** Both faceted paths exist, but old Corax's `CoraxQueryBuilder.BuildQuery` result is **not** `IDisposable`-wrapped; **neither** path disposes or uses try/finally (grep for `IDisposable` in the file → nothing). The "one path fixed, other not" asymmetry is a 2.0 artifact.
- **F-CMP1** (`AbstractIndexCreateController.cs`, compound-field custom-analyzer validation): **N/A.** `TryGetCompoundFirstFieldRejectionReason` / any compound-first-field validation **does not exist** anywhere on this branch (grep → nothing); compound fields here are just materialized as name-pairs in `Index.InitializeCompoundFields()` with no `Indexing`/`Analyzer` validation.
- **F-IVT1** (`CommonAssemblyInfo.*`, `InternalsVisibleTo("Corax.QueryCatalog")`): **N/A.** No `Corax.QueryCatalog` project exists (`find` → nothing) and neither `CommonAssemblyInfo.Linux.cs`/`.Windows.cs` grants it. Policy concern, 2.0-only.
- **F-TST1** (`ExecuteRQLQuery` test-helper duplication): **N/A.** `QueryPlanBuilder`/`BuildFilterMatch`/`ExecuteRQLQuery` don't exist here (grep → nothing); old Corax tests use `CoraxQueryBuilder.BuildQuery`.
- **Confidence:** high (all four verified by direct read + tree-wide grep).

---

## Part 2 — findings whose files do not exist on this branch (Corax 2.0-only → N/A)

All 15 below reference files/dirs absent from v7.2 (`ResidualScanIlEmitter`, `QueryPlanBuilder/*`, `RoaringBitmaps/*`, `BitmapMatch`, `CompiledQuery`, `GraphvizGraph`, `DirectScanMatch`, `TermsProviders/RangePostingBuckets`, `CoraxCostModelCalibration`). They are **not applicable to v7.2**. The "v8 credibility" column is my prior estimate for the PR itself (unverified here since the code is absent).

| # | Finding (file) | v7.2 | v8 credibility (estimate) |
|---|---|---|---|
| 🔴 | NaN doubles pass `>=`/`<=` — ordered `Clt`/`Cgt` (`ResidualScanIlEmitter.cs`) | N/A | **Real** correctness (NaN edge); confirmed mechanism on v8 |
| 🔴 | `when($flag, spatial.within(...))` never evaluated (`BuildResolver.cs`) | N/A | **Real** functional bug on v8 (spatial/vector skip WhenCondition) |
| 🔴 | `BETWEEN(*,*)` → bogus scan-param index → crash (`ScanParamExtractor.cs`) | N/A | Plausible crash on v8 |
| 🔴 | `SumBucketPostings` null `UnmanagedSpan` deref (`RangePostingBuckets.cs`) | N/A | Plausible crash on v8 |
| 🔴 | `ComputeCount` inflated for `ArrayUnsorted`+dups (`RoaringBitmap.cs`) | N/A | Plausible (estimate contract) |
| 🟡 | `AndNotWith` skips `ResolveCardinality` → `IsEmpty` wrong (`RoaringBitmap.cs`) | N/A | Worth confirming on v8 (touches negation) |
| 🟡 | `AndWithRange` stale container-type label (`RoaringBitmap.cs`) | N/A | Plausible edge on v8 |
| 🟡 | `BitmapMatch.Count` no `PrepareForReading` (`BitmapMatch.cs`) | N/A | Plausible on v8 |
| 🟡 | `DirectScanFilteredMatch.Fill` no cancellation (`DirectScanMatch.cs`) | N/A | Real responsiveness gap on v8 |
| 🟡 | Residual-scan IL no cancellation (`ResidualScanIlEmitter.cs`) | N/A | Real responsiveness gap on v8 |
| 🟡 | `EmitDoubleBetween` `Bgt_Un`/`Blt_Un` NaN drift (`ResidualScanIlEmitter.cs`) | N/A | Design-clarify on v8 |
| 🟡 | `CompiledQuery` record-struct copy-dispose dangling ref (`CompiledQuery.cs`) | N/A | Real fragility on v8, currently benign (author-flagged) |
| 🟡 | GraphViz tooltip `\\ ` separator (`GraphvizGraph.cs`) | N/A | **Real** cosmetic on v8 (observed in plan-graph dumps) |
| 🟡 | `WhenCount` undercounted for spatial/vector-only (`QueryPlanBuilder.cs`) | N/A | Real on v8 (same root as WHEN-spatial) |
| 🟡 | `FindNode` missing null guard — test (`CoraxCostModelCalibration.cs`) | N/A | Trivial test-only on v8 |

---

## Applicability tally (answering "how many apply to v7.2")

- **Applicable to v7.2 (code + pattern exist here): 8 findings** — F-VS1, F-SR1 (×2 review items), F-RX1, F-RX2, F-CK1, F-CK2, F-CK3. **All non-critical:** 1 bounded inefficiency (F-VS1), 2 bounded/deliberate (F-RX1/2), 4 benign-perf (F-CK1/2/3), 1 benign (F-SR1). **Zero correctness bugs, zero unbounded leaks, zero must-fix.**
- **Not applicable to v7.2: 20 findings** — 15 in absent Corax 2.0 files + 5 present-file-but-2.0-only-pattern (F-PM1, F-FAC1, F-CMP1, F-IVT1, F-TST1).

## Recommendation for v7.2

No hotfix warranted. If any cleanup is desired: **F-VS1** (route the seed enumerator through `GetOrCreateVectorSearchState`) is the single item with real — if transaction-bounded — value, and should go in with a filtered-vector-search test. The `CompactKey` and regex-cache items are deliberate/`PERF`-sensitive and best left to the author; the rest are N/A.
