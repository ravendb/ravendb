using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

public class LimitEarlyExitTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void SingleClauseWithLimit(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Doc>(
                "from index 'DocIndex' where Tag = 'even' limit 10")
            .ToList();

        Assert.Equal(10, results.Count);
        foreach (var r in results)
            Assert.Equal("even", r.Tag);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task SingleClauseWithLimitReportsOutput(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncDocumentQuery<Doc, DocIndex>()
            .WhereEquals("Tag", "even")
            .Take(10)
            .Timings(out QueryTimings timings)
            .ToListAsync();

        Assert.Equal(10, results.Count);

        var plan = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotNull(plan);
        Assert.Equal("CompiledQuery", plan.Operation);
        Assert.True(plan.Parameters.ContainsKey("Output"),
            $"Output missing. Parameters: {string.Join(", ", plan.Parameters.Keys)}");
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void SingleClauseWithLimitAndOrderBy(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Doc>(
                "from index 'DocIndex' where Tag = 'even' order by Value limit 10")
            .ToList();

        Assert.Equal(10, results.Count);
        for (int i = 1; i < results.Count; i++)
            Assert.True(results[i].Value >= results[i - 1].Value);
        foreach (var r in results)
            Assert.Equal("even", r.Tag);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void OrChainWithLimit(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Doc>(
                "from index 'DocIndex' where Tag = 'even' or Tag = 'odd' limit 10")
            .ToList();

        Assert.Equal(10, results.Count);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task OrChainWithLimitDoesNotScanAll(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncDocumentQuery<Doc, DocIndex>()
            .WhereEquals("Tag", "even")
            .OrElse()
            .WhereEquals("Tag", "odd")
            .Take(10)
            .Timings(out QueryTimings timings)
            .ToListAsync();

        Assert.Equal(10, results.Count);

        var plan = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotNull(plan);
        Assert.Equal("CompiledQuery", plan.Operation);
        var output = long.Parse(plan.Parameters["Output"]);
        Assert.True(output < 500,
            $"Expected early exit to produce fewer than 500 entries, but produced {output}");
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task OrChainWithLimitMarksEarlyExitInGraph(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500); // every doc is even or odd → the OR chain matches all 500, but the limit caps it

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncDocumentQuery<Doc, DocIndex>()
            .WhereEquals("Tag", "even")
            .OrElse()
            .WhereEquals("Tag", "odd")
            .Take(10)
            .Timings(out QueryTimings timings)
            .ToListAsync();

        Assert.Equal(10, results.Count);

        var plan = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotNull(plan);
        Assert.Equal("CompiledQuery", plan.Operation);

        // The pushed-down page limit (10) capped the pipeline below the 500 actual matches, so the run
        // early-exited. OverlayTimings records Limit + EarlyExit on the root, and the dataflow graph labels
        // the slot-0 → result edge so a reader can see we did NOT scan the rest.
        Assert.Equal("10", plan.Parameters["Limit"]);
        Assert.Equal("true", plan.Parameters["EarlyExit"]);
        Assert.Contains("limit=10 (early exit)", plan.Parameters["PlanGraphDot"]);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task NoLimitDoesNotMarkEarlyExit(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 100);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncDocumentQuery<Doc, DocIndex>()
            .WhereEquals("Tag", "even")
            .Timings(out QueryTimings timings)
            .ToListAsync();

        Assert.Equal(50, results.Count);

        var plan = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotNull(plan);
        Assert.Equal("CompiledQuery", plan.Operation);

        // No limit was pushed down, so nothing was skipped: neither the EarlyExit flag nor the edge label appears.
        Assert.False(plan.Parameters.ContainsKey("EarlyExit"));
        Assert.DoesNotContain("early exit", plan.Parameters["PlanGraphDot"]);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void AndChainWithLimit(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Doc>(
                "from index 'DocIndex' where Tag = 'even' and Value < 100 limit 10")
            .ToList();

        Assert.Equal(10, results.Count);
        foreach (var r in results)
        {
            Assert.Equal("even", r.Tag);
            Assert.True(r.Value < 100);
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void LimitLargerThanResultSet(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 20);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Doc>(
                "from index 'DocIndex' where Tag = 'even' limit 100")
            .ToList();

        Assert.Equal(10, results.Count);
        foreach (var r in results)
            Assert.Equal("even", r.Tag);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void LimitOneReturnsExactlyOne(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Doc>(
                "from index 'DocIndex' where Tag = 'even' limit 1")
            .ToList();

        Assert.Single(results);
        Assert.Equal("even", results[0].Tag);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void SkipAndTakeReturnsCorrectPage(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500);

        using var session = store.OpenSession();
        // skip 5, take 10 → bitmap needs at least 15 entries
        var results = session.Advanced.RawQuery<Doc>(
                "from index 'DocIndex' where Tag = 'even' limit 5, 10")
            .ToList();

        Assert.Equal(10, results.Count);
        foreach (var r in results)
            Assert.Equal("even", r.Tag);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task SkipAndTakeReportsOutput(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncDocumentQuery<Doc, DocIndex>()
            .WhereEquals("Tag", "even")
            .Skip(5)
            .Take(10)
            .Timings(out QueryTimings timings)
            .ToListAsync();

        Assert.Equal(10, results.Count);

        var plan = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotNull(plan);
        Assert.Equal("CompiledQuery", plan.Operation);
        Assert.True(plan.Parameters.ContainsKey("Output"),
            $"Output missing. Parameters: {string.Join(", ", plan.Parameters.Keys)}");
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void NoLimitReturnsAll(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 100);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Doc>(
                "from index 'DocIndex' where Tag = 'even'")
            .ToList();

        Assert.Equal(50, results.Count);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task NoLimitScansAll(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 100);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncDocumentQuery<Doc, DocIndex>()
            .WhereEquals("Tag", "even")
            .Timings(out QueryTimings timings)
            .ToListAsync();

        Assert.Equal(50, results.Count);

        var plan = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotNull(plan);
        Assert.Equal("CompiledQuery", plan.Operation);
        var output = long.Parse(plan.Parameters["Output"]);
        Assert.Equal(50, output);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void NegatedEqualsReportsComplementTotal(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500); // Value 0..499 → exactly one doc has Value == 10

        using var session = store.OpenSession();
        // Explicit RQL: `not (Value = 10)` parses to ClauseType.Equals + IsNegated (a NegatedExpression
        // wrapping an Equals), distinct from `Value != 10` (ClauseType.NotEquals) — only this shape stresses
        // the IsNegated guard.
        var results = session.Advanced
            .RawQuery<Doc>("from index 'DocIndex' where true and not (Value = 10)")
            .Statistics(out var stats)
            .ToList();

        // not (Value = 10) parses to ClauseType.Equals + IsNegated: the cardinality estimator keys on
        // ClauseType and stores the term count (1), but the plan executes FillAllEntries AndNot and produces
        // the complement (499). ComputeKnownExactTotal must reject this shape (the IsNegated guard on the
        // Equals branch) and fall back to scanning, so the reported total is the true complement — not the
        // term count. Drop that guard and TotalResults wrongly reports 1.
        Assert.Equal(499, results.Count);
        Assert.Equal(499, stats.TotalResults);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task WhenFalseGuardCollapsesToAllEntriesAndEarlyExits(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500); // when(false, ...) drops the predicate → the query matches every doc

        using var session = store.OpenAsyncSession();
        // when($flag = true, Tag = $t) with flag=false: the guard fails, so the Tag predicate is dropped and the
        // top-level clause statically collapses to a lone MatchAll sentinel. That sentinel already carries its
        // exact O(1) count (NumberOfEntries) in Cardinality, so ComputeKnownExactTotal must report it directly
        // instead of draining the full index to count it. With the known total in hand the page limit (10) is
        // pushed down → the pipeline early-exits and TotalResults is the exact whole-index count.
        var results = await session.Advanced
            .AsyncRawQuery<Doc>("from index 'DocIndex' where when($flag = true, Tag = $t) include timings() limit 10")
            .AddParameter("flag", false)
            .AddParameter("t", "even")
            .Statistics(out var stats)
            .Timings(out QueryTimings timings)
            .ToListAsync();

        Assert.Equal(10, results.Count);
        Assert.Equal(500, stats.TotalResults); // the lone sentinel's exact count, not a drained scan

        var plan = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotNull(plan);
        Assert.Equal("CompiledQuery", plan.Operation);
        Assert.Equal("10", plan.Parameters["Limit"]);
        Assert.Equal("true", plan.Parameters["EarlyExit"]);
        Assert.Contains("limit=10 (early exit)", plan.Parameters["PlanGraphDot"]);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task FullScanOrderByReportsExactTotalFromPostingCount(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500); // Value 0..499, single-valued

        using var session = store.OpenAsyncSession();
        // No WHERE: the sorted scan drives off the full Value range (DirectScanSimpleMatch). For a single-valued
        // field the exact TotalResults equals the driving provider's posting count (500), resolved up front from
        // CountPostingsInRange instead of draining Fill to recount — see DirectScanMatchBase.KnownExactTotal.
        var results = await session.Advanced
            .AsyncDocumentQuery<Doc, DocIndex>()
            .OrderBy(x => x.Value)
            .Take(10)
            .Statistics(out var stats)
            .Timings(out QueryTimings timings)
            .ToListAsync();

        Assert.Equal(10, results.Count);
        for (int i = 1; i < results.Count; i++)
            Assert.True(results[i].Value >= results[i - 1].Value);
        Assert.Equal(0, results[0].Value); // smallest Value first
        Assert.Equal(500, stats.TotalResults);

        var plan = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotNull(plan);
        Assert.True(plan.Parameters.TryGetValue("PlanGraphDot", out var dot) && dot.Contains("data_knownexacttotal=\"500\""), dot);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task RangeDrivingOrderByReportsExactTotalFromPostingCount(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500); // Value 0..499 → 399 docs have Value > 100

        using var session = store.OpenAsyncSession();
        // where Value > 100 order by Value: the range clause drives the sort with no residual (DirectScanSimpleMatch).
        // Single-valued, so the exact TotalResults is the in-range posting count (399), read from the provider
        // rather than draining Fill — the page (10) is returned without walking the remaining 389 matches to count.
        var results = await session.Advanced
            .AsyncDocumentQuery<Doc, DocIndex>()
            .WhereGreaterThan(x => x.Value, 100)
            .OrderBy(x => x.Value)
            .Take(10)
            .Statistics(out var stats)
            .Timings(out QueryTimings timings)
            .ToListAsync();

        Assert.Equal(10, results.Count);
        for (int i = 1; i < results.Count; i++)
            Assert.True(results[i].Value >= results[i - 1].Value);
        Assert.Equal(101, results[0].Value); // smallest Value strictly greater than 100
        Assert.Equal(399, stats.TotalResults);

        var plan = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotNull(plan);
        Assert.True(plan.Parameters.TryGetValue("PlanGraphDot", out var dot) && dot.Contains("data_knownexacttotal=\"399\""), dot);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task FullScanOrderByStringWithNullsReportsExactTotalIncludingNulls(Options options)
    {
        using var store = GetDocumentStore(options);
        new DocIndex().Execute(store);

        const int withTag = 300;
        const int nullTag = 200;
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < withTag; i++)
                bulk.Store(new Doc { Value = i, Tag = "t" + (i % 50) });
            for (int i = 0; i < nullTag; i++)
                bulk.Store(new Doc { Value = withTag + i, Tag = null });
        }

        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        // Full scan ORDER BY a string field drives off the exists provider, which is built skipNulls because
        // SortedDrivingMatch emits the field's null (and non-existing) groups itself. The exact TotalResults must
        // therefore include those null docs: it is the index entry count (500), NOT the exists provider's
        // null-excluded posting sum (300). Asserting the known-total plan value pins the count to the
        // page-bounded fast path rather than a Fill drain (which would also read 500 and hide a regression).
        var results = await session.Advanced
            .AsyncDocumentQuery<Doc, DocIndex>()
            .OrderBy(x => x.Tag)
            .Take(10)
            .Statistics(out var stats)
            .Timings(out QueryTimings timings)
            .ToListAsync();

        Assert.Equal(10, results.Count);
        Assert.Equal(withTag + nullTag, stats.TotalResults);

        var plan = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotNull(plan);
        Assert.True(plan.Parameters.TryGetValue("PlanGraphDot", out var dot) &&
                    dot.Contains($"data_knownexacttotal=\"{withTag + nullTag}\""), dot);
    }

    private void InsertDocuments(IDocumentStore store, int count)
    {
        new DocIndex().Execute(store);

        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < count; i++)
                bulk.Store(new Doc { Value = i, Tag = i % 2 == 0 ? "even" : "odd" });
        }

        Indexes.WaitForIndexing(store);
    }

    private class DocIndex : AbstractIndexCreationTask<Doc>
    {
        public DocIndex()
        {
            Map = docs => from d in docs select new { d.Value, d.Tag };
        }
    }

    private class Doc
    {
        public string Id { get; set; }
        public int Value { get; set; }
        public string Tag { get; set; }
    }
}
