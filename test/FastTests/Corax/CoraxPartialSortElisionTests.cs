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
/// Partial sort elision (RavenDB-25281). An ORDER BY key that is pinned to a constant by a top-level
/// equality WHERE clause AND is single-valued in the index is constant across every result, so it
/// contributes nothing to the order and is dropped at plan-build time (ComputeEffectiveOrderBy). The
/// remaining keys keep their relative precedence, so <c>WHERE Name = 'Bob' ORDER BY Name, Seq</c> reduces
/// to <c>ORDER BY Seq</c>. This turns the leading sort key into a residual filter and lets the scan be
/// driven by the surviving key — observable both in the plan (the DirectScan drives on Seq, not Name) and
/// in the results (identical rows, also matched against Lucene which has no elision).
/// </summary>
public class CoraxPartialSortElisionTests : RavenTestBase
{
    public CoraxPartialSortElisionTests(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int Seq { get; set; }
    }

    private class Items_Index : AbstractIndexCreationTask<Item>
    {
        public Items_Index()
        {
            Map = items => from i in items
                select new { i.Name, i.Seq };
        }
    }

    // Name is heavily skewed to "Bob" (3 of 4) so a Name='Bob' residual is non-selective, keeping the
    // page-bounded DirectScan estimate small enough to win the cost gate. Seq is unique and ascending.
    private static List<Item> BuildSeed(int count)
    {
        string[] names = { "Bob", "Bob", "Bob", "Alice" };
        var items = new List<Item>(count);
        for (int i = 0; i < count; i++)
            items.Add(new Item { Id = $"items/{i}", Name = names[i % names.Length], Seq = i });
        return items;
    }

    private static async Task SeedAsync(IDocumentStore store, List<Item> items)
    {
        using var bulk = store.BulkInsert();
        foreach (var it in items)
            await bulk.StoreAsync(it, it.Id);
    }

    // Plan proof: with the leading pinned single-valued key (Name) elided, the effective ORDER BY is just
    // Seq, so the page-bounded scan is driven by Seq's tree with Name demoted to a residual predicate. Were
    // the key NOT elided, the primary sort field would be Name and the scan would drive on Name's tree.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task PinnedLeadingKey_IsElided_ScanDrivesOnSurvivingKey(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);
        var items = BuildSeed(4000);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        // No statistics requested -> SkipStatistics=true -> the scan is page-bounded (take=25) and DirectScan
        // wins the gate. The leading ORDER BY key Name is pinned by Name='Bob' and single-valued, so it is elided.
        var results = await session.Advanced
            .AsyncRawQuery<Item>($"from index '{index.IndexName}' where Seq between 0 and 3999 and Name = 'Bob' " +
                                 "order by Name, Seq as long limit 25 include timings()")
            .Timings(out var timings)
            .ToListAsync();

        Assert.NotEmpty(results);
        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        Assert.True(plan.Parameters.TryGetValue("OptimizationHint", out var hint) && hint == "FieldSortedScan",
            "Expected FieldSortedScan after eliding the pinned leading key, but OptimizationHint was '" +
            (hint ?? "<missing>") + "'. Plan: " + Describe(plan));

        var compiled = FindOperation(plan, "CompiledQuery");
        var directScan = compiled?.Children?.FirstOrDefault(c => c.Operation == "DirectScan");
        Assert.True(directScan != null, "Expected a DirectScan node. Plan: " + Describe(plan));
        Assert.Equal("Seq", directScan.Parameters["DrivingTree"]);
        Assert.True(directScan.Parameters.TryGetValue("ResidualPredicates", out var residuals) && residuals.Contains("Name"),
            "Expected the elided leading key 'Name' to become a residual predicate. Plan: " + Describe(plan));
    }

    // Correctness: eliding the constant leading key must not change the result set or order. Name is constant
    // ('Bob') across the matches, so ORDER BY Name, Seq is equivalent to ORDER BY Seq. Checked against a
    // brute-force expectation and across engines (Lucene has no elision, so a match proves equivalence).
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task PinnedLeadingKey_Elision_PreservesResults(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);
        var items = BuildSeed(4000);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Item>($"from index '{index.IndexName}' where Seq between 0 and 3999 and Name = 'Bob' " +
                                 "order by Name, Seq as long limit 25")
            .ToListAsync();

        var actual = results.Select(r => r.Id).ToList();
        var expected = items
            .Where(i => i.Seq >= 0 && i.Seq <= 3999 && string.Equals(i.Name, "Bob", StringComparison.OrdinalIgnoreCase))
            .OrderBy(i => i.Seq)
            .Take(25)
            .Select(i => i.Id)
            .ToList();

        Assert.NotEmpty(expected);
        Assert.Equal(expected, actual);
    }

    // When EVERY ORDER BY key is pinned + single-valued, partial elision removes them all and the query runs
    // as a plain (sortless) match. Results must still be the full correct set (order-insensitive compare).
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task AllKeysPinned_FullyElides_PreservesResults(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);
        var items = BuildSeed(400);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Item>($"from index '{index.IndexName}' where Name = 'Alice' order by Name")
            .ToListAsync();

        var actual = results.Select(r => r.Id).OrderBy(x => x).ToList();
        var expected = items
            .Where(i => string.Equals(i.Name, "Alice", StringComparison.OrdinalIgnoreCase))
            .Select(i => i.Id)
            .OrderBy(x => x)
            .ToList();

        Assert.NotEmpty(expected);
        Assert.Equal(expected, actual);
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
