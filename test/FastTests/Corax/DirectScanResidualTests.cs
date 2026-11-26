using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

/// <summary>
/// Regression guards for the direct-scan residual path (RavenDB-25281). A direct-scan plan is driven by an
/// ORDER BY field that also carries a range/equals WHERE clause; the remaining WHERE clauses become per-entry
/// residuals evaluated by <c>CompiledEntryPredicate</c>. Two historical bugs:
///   1. residual string-equality terms were not analyzer-encoded, so a mixed-case value (e.g. <c>Name = 'BOB'</c>)
///      never matched the stored, lower-cased term <c>bob</c>;
///   2. an <c>(A or B)</c> group residual was not handled recursively, throwing/returning wrong rows.
/// Both are fixed by routing entry-scan and direct-scan through one recursive core (ScanParamExtractor.
/// ExtractFromPredicate, always analyzing via GetAnalyzedSlice). Results are checked against brute force and
/// across engines (Lucene has no direct scan, so a match proves the rewrite is semantics-preserving).
/// </summary>
public class DirectScanResidualTests : RavenTestBase
{
    public DirectScanResidualTests(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Category { get; set; }
        public int Seq { get; set; }
    }

    private class Items_Index : AbstractIndexCreationTask<Item>
    {
        public Items_Index()
        {
            Map = items => from i in items
                select new { i.Name, i.Category, i.Seq };
        }
    }

    // Deterministic seed sized to make the direct-scan path cost-effective. DirectScan is a top-N
    // streaming optimization, so it only wins with a small page AND a NON-selective residual (a high
    // pass-rate keeps the scanned-entry estimate small). Name is heavily skewed to "Bob" (3 of 4) so a
    // Name='BOB' residual is non-selective; Category cycles 3-way so an (red or blue) group residual is
    // ~2/3 non-selective. Both keep the per-execution cost gate on the DirectScan side. Name is
    // capitalised on purpose so the residual must lower-case it to match the stored term.
    private static List<Item> BuildSeed(int count)
    {
        string[] names = { "Bob", "Bob", "Bob", "Alice" };
        string[] cats = { "red", "green", "blue" };
        var items = new List<Item>(count);
        for (int i = 0; i < count; i++)
        {
            items.Add(new Item
            {
                Id = $"items/{i}",
                Name = names[i % names.Length],
                Category = cats[i % cats.Length],
                Seq = i
            });
        }

        return items;
    }

    private static async Task SeedAsync(IDocumentStore store, List<Item> items)
    {
        using var bulk = store.BulkInsert();
        foreach (var it in items)
            await bulk.StoreAsync(it, it.Id);
    }

    // Top-N expectation: the lowest-Seq matches, in Seq order, capped at limit (matches "order by Seq
    // as long limit N"). The residual code path under test is only exercised when DirectScan is chosen,
    // which requires the small page bound — so the correctness checks must use the same top-N shape.
    private static List<string> ExpectedTopN(IEnumerable<Item> items, Func<Item, bool> predicate, int limit) =>
        items.Where(predicate).OrderBy(i => i.Seq).Take(limit).Select(i => i.Id).ToList();

    // Bug 1: a direct-scan residual string-equality with a mixed-case value. Stored term is lower-cased
    // by the default analyzer ("Bob" -> "bob"); the residual must analyzer-encode 'BOB' the same way.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task DirectScanResidual_MixedCaseStringEquality_Matches(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);
        var items = BuildSeed(4000);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Item>($"from index '{index.IndexName}' where Seq between 0 and 3999 and Name = 'BOB' order by Seq as long limit 25")
            .ToListAsync();

        var actual = results.Select(r => r.Id).ToList();
        var expected = ExpectedTopN(items,
            i => i.Seq >= 0 && i.Seq <= 3999 && string.Equals(i.Name, "BOB", StringComparison.OrdinalIgnoreCase), 25);

        Assert.NotEmpty(expected);
        Assert.Equal(expected, actual);
    }

    // Bug 2: a direct-scan plan with an (A or B) group residual must recurse into the group, returning
    // correct rows without an IndexOutOfRangeException.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task DirectScanResidual_OrGroup_Matches(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);
        var items = BuildSeed(4000);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Item>($"from index '{index.IndexName}' where Seq between 0 and 3999 and (Category = 'red' or Category = 'blue') order by Seq as long limit 25")
            .ToListAsync();

        var actual = results.Select(r => r.Id).ToList();
        var expected = ExpectedTopN(items,
            i => i.Seq >= 0 && i.Seq <= 3999 && (i.Category == "red" || i.Category == "blue"), 25);

        Assert.NotEmpty(expected);
        Assert.Equal(expected, actual);
    }

    // Proves the direct-scan path is actually exercised (not silently demoted to a bitmap sort) so the
    // two correctness tests above are guarding the intended code. Corax-only: Lucene has no DirectScan.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task DirectScanResidual_PlanUsesDirectScan(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);
        var items = BuildSeed(4000);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Item>($"from index '{index.IndexName}' where Seq between 0 and 3999 and Name = 'BOB' order by Seq as long limit 25 include timings()")
            .Timings(out var timings)
            .ToListAsync();

        Assert.NotEmpty(results);
        Assert.NotNull(timings);
        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        var compiled = FindOperation(plan, "CompiledQuery");
        Assert.True(compiled != null, "Expected a CompiledQuery node in the plan. Plan: " + Describe(plan));
        Assert.True(compiled.Parameters.TryGetValue("OptimizationHint", out var hint) && hint == "FieldSortedScan",
            "Expected the plan to use the FieldSortedScan strategy, but OptimizationHint was '" + (hint ?? "<missing>") + "'. Plan: " + Describe(plan));

        // The executed scan match's own structure must be surfaced under the plan (the bitmap op template never
        // ran for this query). The node carries the driving tree, residual predicates, and per-run scan counts,
        // attached as a DIRECT child of CompiledQuery, keeping the match's own name "DirectScan" — distinct from
        // the DecisionTrail's "FieldSortedScan" candidacy entry. Target the structural node by its child position.
        var directScan = compiled.Children?.FirstOrDefault(c => c.Operation == "DirectScan");
        Assert.True(directScan != null, "Expected a DirectScan node as a direct child of CompiledQuery. Plan: " + Describe(plan));
        Assert.True(directScan.Parameters.ContainsKey("DrivingTree"),
            "DirectScan params: " + string.Join(", ", directScan.Parameters.Select(kv => kv.Key + "=" + kv.Value)));
        Assert.Equal("Seq", directScan.Parameters["DrivingTree"]);
        Assert.True(directScan.Parameters.TryGetValue("ResidualPredicates", out var residuals) && residuals.Contains("Name"),
            "Expected the surfaced DirectScan node to carry the 'Name' residual predicate. Plan: " + Describe(plan));
        Assert.True(directScan.Parameters.ContainsKey("TreeEntriesScanned"));
    }

    // Cost-model regression (RavenDB-25281). A sorted residual scan only early-terminates at the page boundary
    // when its take is page-bounded. Requesting statistics (or a count query, or a post-filter) forces the scan
    // to TakeAll so it can report an exact TotalResults — it then reads every matching entry's stored fields.
    // The DirectScan cost gate must therefore price the scan against the FULL matching set, not the limit-1 page.
    // Before the fix the estimate always assumed the page bound, so a "ORDER BY non-selective-field DESC LIMIT 1
    // + residual + statistics" query chose DirectScan and drained the whole driving tree (TreeExhausted, slow).
    // This pins the contrast: the SAME query picks DirectScan without statistics (page-bounded, a few entries)
    // and the bitmap pipeline with statistics (no stored-entry read per scanned entry), returning identical rows.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task DirectScanResidual_StatisticsRequested_RejectsFullTreeScan(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);
        var items = BuildSeed(4000);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        string query = $"from index '{index.IndexName}' where Seq between 0 and 3999 and Name = 'BOB' " +
                       "order by Seq as long desc limit 1 include timings()";

        using var session = store.OpenAsyncSession();

        // No statistics requested -> client sends SkipStatistics=true -> the sorted scan is page-bounded
        // (take=1), so DirectScan wins the cost gate and the plan reports the FieldSortedScan strategy.
        var noStatsResults = await session.Advanced
            .AsyncRawQuery<Item>(query)
            .Timings(out var noStatsTimings)
            .ToListAsync();

        var noStatsPlan = noStatsTimings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(noStatsPlan);
        Assert.True(noStatsPlan.Parameters.TryGetValue("OptimizationHint", out var noStatsHint) && noStatsHint == "FieldSortedScan",
            "Without statistics the page-bounded scan should pick FieldSortedScan, but OptimizationHint was '" +
            (noStatsHint ?? "<missing>") + "'. Plan: " + Describe(noStatsPlan));

        // Statistics requested -> SkipStatistics=false -> the read must drain the driving tree to count
        // TotalResults, so DirectScan would do a stored-entry read per matching entry. The cost model now
        // prices it against the full matching set and rejects it, falling back to the bitmap pipeline.
        var statsResults = await session.Advanced
            .AsyncRawQuery<Item>(query)
            .Statistics(out var stats)
            .Timings(out var statsTimings)
            .ToListAsync();

        var statsPlan = statsTimings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(statsPlan);
        statsPlan.Parameters.TryGetValue("OptimizationHint", out var statsHint);
        Assert.True(statsHint != "FieldSortedScan",
            "With statistics the scan is TakeAll (full tree drain), so DirectScan must be rejected for the bitmap " +
            "pipeline, but OptimizationHint was 'FieldSortedScan'. Plan: " + Describe(statsPlan));
        var compiled = FindOperation(statsPlan, "CompiledQuery");
        Assert.True(compiled?.Children?.FirstOrDefault(c => c.Operation == "DirectScan") == null,
            "Expected NO DirectScan node when statistics are requested. Plan: " + Describe(statsPlan));

        // Both shapes return the identical correct top-1 row, and statistics report the true total match count.
        Assert.Equal(noStatsResults.Select(r => r.Id), statsResults.Select(r => r.Id));
        long expectedTotal = items.Count(i => i.Seq >= 0 && i.Seq <= 3999 &&
                                              string.Equals(i.Name, "BOB", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(expectedTotal, stats.TotalResults);
    }

    private static QueryInspectionNode FindOperation(QueryInspectionNode node, string operation)
    {
        if (node == null)
            return null;
        if (node.Operation == operation)
            return node;
        if (node.Children == null)
            return null;
        foreach (var child in node.Children)
        {
            var found = FindOperation(child, operation);
            if (found != null)
                return found;
        }

        return null;
    }

    private static string Describe(QueryInspectionNode node, int depth = 0)
    {
        if (node == null)
            return "<null>";
        var prefix = new string(' ', depth * 2);
        var line = prefix + node.Operation;
        if (node.Children == null || node.Children.Count == 0)
            return line;
        return line + Environment.NewLine + string.Join(Environment.NewLine, node.Children.Select(c => Describe(c, depth + 1)));
    }
}
