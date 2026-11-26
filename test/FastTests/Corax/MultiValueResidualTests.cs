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
/// Regression guards for residual scans over MULTI-VALUED fields (RavenDB-25281). A residual predicate
/// is evaluated per entry by the IL-emitted <c>CompiledEntryPredicate</c>. The value comparisons walk the
/// field's terms via <c>reader.FindNext</c>; the bug being pinned here is that equality/relational and
/// NotEqual predicates used to read only the FIRST term, so a multi-valued field whose matching (or, for
/// NotEqual, equal) value was not the first term gave wrong results.
///
/// The seed makes the relevant value always a NON-first term (every doc stores a small value 0..4 first,
/// then 7 / 99 / "zzz"), so a single-FindNext implementation would miss it. Each query is shaped to take
/// the direct-scan residual path (ORDER BY Seq as long + a Seq range driver + the multi-value clause as a
/// non-selective residual), and the result is checked against a brute-force expectation across engines
/// (Corax vs Lucene — Lucene has no direct scan, so a match proves the rewrite is semantics-preserving).
/// </summary>
public class MultiValueResidualTests : RavenTestBase
{
    public MultiValueResidualTests(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Id { get; set; }
        public int[] Scores { get; set; }
        public string[] Tags { get; set; }
        public int Seq { get; set; }
    }

    private class Items_Index : AbstractIndexCreationTask<Item>
    {
        public Items_Index()
        {
            Map = items => from i in items
                select new { i.Scores, i.Tags, i.Seq };
        }
    }

    // Each doc stores TWO values per multi-value field. The first is always a small value (0..4), strictly
    // less than the values queried (7, 99) and lexically less than "zzz" — so the queried value is never the
    // first term. 7 / "zzz" are present in 3 of 4 docs; 99 / "yyy" in the remaining 1 of 4. Both equality
    // ('=7') and NotEqual ('!=99') therefore land on a 3/4-pass-rate residual (non-selective, keeping the
    // direct-scan cost gate satisfied) while exercising a NON-first matching term.
    private static List<Item> BuildSeed(int count)
    {
        var items = new List<Item>(count);
        for (int i = 0; i < count; i++)
        {
            int small = i % 5; // 0..4, always < 7 < 99
            bool common = i % 4 != 0; // 3/4 of docs
            items.Add(new Item
            {
                Id = $"items/{i}",
                Scores = common ? new[] { small, 7 } : new[] { small, 99 },
                Tags = common ? new[] { "aaa", "zzz" } : new[] { "aaa", "yyy" },
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

    private static List<string> ExpectedTopN(IEnumerable<Item> items, Func<Item, bool> predicate, int limit) =>
        items.Where(predicate).OrderBy(i => i.Seq).Take(limit).Select(i => i.Id).ToList();

    // Positive numeric equality on a multi-value field: the matching value (7) is the SECOND term.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task MultiValueResidual_NumericEquality_Matches(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);
        var items = BuildSeed(4000);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Item>($"from index '{index.IndexName}' where Seq between 0 and 3999 and Scores = 7 order by Seq as long limit 25")
            .ToListAsync();

        var actual = results.Select(r => r.Id).ToList();
        var expected = ExpectedTopN(items, i => i.Seq >= 0 && i.Seq <= 3999 && i.Scores.Contains(7), 25);

        Assert.NotEmpty(expected);
        Assert.Equal(expected, actual);
    }

    // NotEqual on a multi-value field: the EXCLUDED value (99) is the SECOND term, so the entry must be
    // rejected even though its first term differs. A single-FindNext NotEqual would wrongly keep it.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task MultiValueResidual_NumericNotEqual_Matches(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);
        var items = BuildSeed(4000);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Item>($"from index '{index.IndexName}' where Seq between 0 and 3999 and Scores != 99 order by Seq as long limit 25")
            .ToListAsync();

        var actual = results.Select(r => r.Id).ToList();
        var expected = ExpectedTopN(items, i => i.Seq >= 0 && i.Seq <= 3999 && i.Scores.Contains(99) == false, 25);

        Assert.NotEmpty(expected);
        Assert.Equal(expected, actual);
    }

    // Positive string equality on a multi-value field: the matching term ("zzz") is the SECOND term and is
    // lower-cased by the default analyzer, so the residual must both walk all terms and analyzer-encode.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task MultiValueResidual_StringEquality_Matches(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);
        var items = BuildSeed(4000);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Item>($"from index '{index.IndexName}' where Seq between 0 and 3999 and Tags = 'zzz' order by Seq as long limit 25")
            .ToListAsync();

        var actual = results.Select(r => r.Id).ToList();
        var expected = ExpectedTopN(items, i => i.Seq >= 0 && i.Seq <= 3999 && i.Tags.Contains("zzz"), 25);

        Assert.NotEmpty(expected);
        Assert.Equal(expected, actual);
    }

    // Proves the multi-value equality query is actually evaluated as a direct-scan residual (the path that
    // contains the FindNext term-walk under test), not silently demoted to a bitmap sort. Corax-only.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task MultiValueResidual_PlanUsesDirectScan(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);
        var items = BuildSeed(4000);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Item>($"from index '{index.IndexName}' where Seq between 0 and 3999 and Scores = 7 order by Seq as long limit 25 include timings()")
            .Timings(out var timings)
            .ToListAsync();

        Assert.NotEmpty(results);
        Assert.NotNull(timings);
        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        var compiled = FindOperation(plan, "CompiledQuery");
        Assert.True(compiled != null, "Expected a CompiledQuery node in the plan. Plan: " + Describe(plan));

        var directScan = compiled.Children?.FirstOrDefault(c => c.Operation == "DirectScan");
        Assert.True(directScan != null, "Expected a DirectScan node as a direct child of CompiledQuery. Plan: " + Describe(plan));
        Assert.Equal("Seq", directScan.Parameters["DrivingTree"]);
        Assert.True(directScan.Parameters.TryGetValue("ResidualPredicates", out var residuals) && residuals.Contains("Scores"),
            "Expected the surfaced DirectScan node to carry the 'Scores' residual predicate. Plan: " + Describe(plan));
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
