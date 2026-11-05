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
/// Coverage for RavenDB-26749's two single-valued optimizations, both gated on
/// <c>IndexSearcher.HasMultipleTermsInField</c> and made cache-safe by folding the single-valued bit
/// into the plan cache key (so a field that flips single-&gt;multi re-plans instead of reusing a baked
/// optimization):
///   1. straight-line residual IL leaf scan (one read + one compare, no per-entry term loop) for
///      single-valued fields, versus the looping path for multi-valued fields;
///   2. sort-key elision: <c>WHERE field = $c ORDER BY field</c> drops the SortingMatch wrapper when
///      <c>field</c> is single-valued, because the equality pins every result to one value.
/// Correctness is checked against a brute-force expectation; the elision/non-elision decision is read
/// back from the query plan (SortingMatch present or absent).
/// </summary>
public class RavenDB_26749 : RavenTestBase
{
    public RavenDB_26749(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Id { get; set; }
        public string Status { get; set; }   // single-valued scalar string
        public int Score { get; set; }       // single-valued scalar int
        public string[] Tags { get; set; }   // multi-valued (array -> always multi-valued)
    }

    // Same field shape as Item but Status is an array, used to flip the Status field to multi-valued.
    private class ItemMv
    {
        public string Id { get; set; }
        public string[] Status { get; set; }
        public int Score { get; set; }
        public string[] Tags { get; set; }
    }

    // Id-only result DTO (Id is populated from @id metadata) for reading back docs whose Status field
    // shape is no longer a plain string.
    private class IdOnly
    {
        public string Id { get; set; }
    }

    // Dedicated model for the residual-scan flip test: Code is the residual-predicate field, Score is
    // the driving ORDER BY field.
    private class Widget
    {
        public string Id { get; set; }
        public string Code { get; set; }   // single-valued scalar
        public int Score { get; set; }
    }

    // Same collection ("Widgets") but Code is an array, flipping Code to multi-valued index-wide.
    private class WidgetMv
    {
        public string Id { get; set; }
        public string[] Code { get; set; }
        public int Score { get; set; }
    }

    private class Widget_Index : AbstractIndexCreationTask<Widget>
    {
        public Widget_Index()
        {
            Map = items => from i in items
                select new { i.Code, i.Score };
        }
    }

    private class Items_Index : AbstractIndexCreationTask<Item>
    {
        public Items_Index()
        {
            Map = items => from i in items
                select new { i.Status, i.Score, i.Tags };
        }
    }

    private static List<Item> BuildSeed(int count)
    {
        string[] statuses = { "active", "inactive", "pending", "archived" };
        var items = new List<Item>(count);
        for (int i = 0; i < count; i++)
        {
            items.Add(new Item
            {
                Id = $"items/{i}",
                Status = statuses[i % statuses.Length],
                Score = i,
                // Each doc carries 2 tags so the field is unambiguously multi-valued; "common" is on
                // every 3rd doc so a Tags='common' residual is non-selective.
                Tags = (i % 3 == 0) ? new[] { "common", $"t{i}" } : new[] { $"t{i}", $"u{i}" }
            });
        }

        return items;
    }

    private static async Task SeedAsync(IDocumentStore store, IEnumerable<Item> items)
    {
        using var bulk = store.BulkInsert();
        foreach (var it in items)
            await bulk.StoreAsync(it, it.Id);
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

    // The strategy that actually ran, surfaced on the CompiledQuery node. "BitmapPipeline" has no
    // inherent ordering; the sorted-scan strategies (FieldSortedScan/CompoundSortedScan/CompoundKeyLookup)
    // bake the ORDER BY into the scan.
    private static string Hint(QueryInspectionNode plan)
    {
        var compiled = FindOperation(plan, "CompiledQuery");
        return compiled != null && compiled.Parameters.TryGetValue("OptimizationHint", out var h) ? h : null;
    }

    // A sort is applied when a SortingMatch wraps the pipeline OR a sorted-scan strategy ran (anything
    // other than the unordered BitmapPipeline).
    private static bool SortApplied(QueryInspectionNode plan) =>
        FindOperation(plan, "SortingMatch") != null || Hint(plan) != "BitmapPipeline";

    // The sort is elided when the pipeline carries no ordering at all: BitmapPipeline, no SortingMatch.
    private static bool SortElided(QueryInspectionNode plan) =>
        FindOperation(plan, "SortingMatch") == null && Hint(plan) == "BitmapPipeline";

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

    // ---- residual IL leaf scan: single-valued straight-line path ----

    // A driving ORDER BY Score range plus a residual equality on the single-valued Status field. The
    // residual predicate compiles to the straight-line single-read/single-compare IL.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task Residual_SingleValuedEquality_Matches(Options options)
    {
        using var store = GetDocumentStore(options);
        new Items_Index().Execute(store);
        var items = BuildSeed(4000);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Item>("from index 'Items/Index' where Score between 0 and 3999 and Status = 'active' order by Score as long limit 25")
            .ToListAsync();

        var actual = results.Select(r => r.Id).ToList();
        var expected = items.Where(i => i.Status == "active").OrderBy(i => i.Score).Take(25).Select(i => i.Id).ToList();

        Assert.NotEmpty(expected);
        Assert.Equal(expected, actual);
    }

    // The straight-line NotEqual path: residual <c>Status != 'active'</c> on the single-valued field.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task Residual_SingleValuedNotEqual_Matches(Options options)
    {
        using var store = GetDocumentStore(options);
        new Items_Index().Execute(store);
        var items = BuildSeed(4000);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Item>("from index 'Items/Index' where Score between 0 and 3999 and Status != 'active' order by Score as long limit 25")
            .ToListAsync();

        var actual = results.Select(r => r.Id).ToList();
        var expected = items.Where(i => i.Status != "active").OrderBy(i => i.Score).Take(25).Select(i => i.Id).ToList();

        Assert.NotEmpty(expected);
        Assert.Equal(expected, actual);
    }

    // ---- residual IL leaf scan: multi-valued looping path ----

    // The residual equality on the multi-valued Tags field must walk every term per entry (the loop IL),
    // matching when ANY term equals the value.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task Residual_MultiValuedEquality_Matches(Options options)
    {
        using var store = GetDocumentStore(options);
        new Items_Index().Execute(store);
        var items = BuildSeed(4000);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Item>("from index 'Items/Index' where Score between 0 and 3999 and Tags = 'common' order by Score as long limit 25")
            .ToListAsync();

        var actual = results.Select(r => r.Id).ToList();
        var expected = items.Where(i => i.Tags.Contains("common")).OrderBy(i => i.Score).Take(25).Select(i => i.Id).ToList();

        Assert.NotEmpty(expected);
        Assert.Equal(expected, actual);
    }

    // ---- sort-key elision ----

    // WHERE Status = 'active' ORDER BY Status: the equality pins Status to one value, and Status is
    // single-valued, so the whole sort is a no-op and the SortingMatch wrapper is dropped.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task SortElision_SingleKeyEqualityPinned_DropsSortingMatch(Options options)
    {
        using var store = GetDocumentStore(options);
        new Items_Index().Execute(store);
        var items = BuildSeed(800);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Item>("from index 'Items/Index' where Status = 'active' order by Status include timings()")
            .Timings(out var timings)
            .ToListAsync();

        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        Assert.True(SortElided(plan), "Expected the sort to be elided (BitmapPipeline, no SortingMatch). Plan: " + Describe(plan));

        var expected = items.Where(i => i.Status == "active").Select(i => i.Id).OrderBy(x => x).ToList();
        var actual = results.Select(r => r.Id).OrderBy(x => x).ToList();
        Assert.Equal(expected, actual);
    }

    // WHERE Status = 'active' ORDER BY Status, Score: Status is pinned but Score is not, so not every
    // ORDER BY key is eliminated -> the sort is kept (SortingMatch present) and results stay Score-ordered.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task SortElision_SecondaryKeyUnpinned_KeepsSort(Options options)
    {
        using var store = GetDocumentStore(options);
        new Items_Index().Execute(store);
        var items = BuildSeed(800);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Item>("from index 'Items/Index' where Status = 'active' order by Status, Score as long include timings()")
            .Timings(out var timings)
            .ToListAsync();

        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        Assert.True(SortApplied(plan), "Expected the sort to be applied (Score unpinned). Plan: " + Describe(plan));

        var expected = items.Where(i => i.Status == "active").OrderBy(i => i.Score).Select(i => i.Id).ToList();
        var actual = results.Select(r => r.Id).ToList();
        Assert.Equal(expected, actual);
    }

    // WHERE Tags = 'common' ORDER BY Tags: Tags is equality-pinned but multi-valued, so a matching doc
    // can still carry other terms -> the sort is NOT a no-op and must be kept.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task SortNoElision_MultiValuedField_KeepsSort(Options options)
    {
        using var store = GetDocumentStore(options);
        new Items_Index().Execute(store);
        var items = BuildSeed(800);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Item>("from index 'Items/Index' where Tags = 'common' order by Tags include timings()")
            .Timings(out var timings)
            .ToListAsync();

        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        Assert.True(SortApplied(plan), "Expected the sort to be applied (Tags is multi-valued). Plan: " + Describe(plan));

        var expected = items.Where(i => i.Tags.Contains("common")).Select(i => i.Id).OrderBy(x => x).ToList();
        var actual = results.Select(r => r.Id).OrderBy(x => x).ToList();
        Assert.Equal(expected, actual);
    }

    // The single->multi flip: the cache key carries the single-valued bit, so when the field becomes
    // multi-valued the elided plan is NOT reused (cache miss re-plans), the sort comes back, and results
    // remain correct.
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task SortElision_FlipToMultiValued_ReplansAndKeepsSort()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        new Items_Index().Execute(store);

        // Phase 1: Status is strictly single-valued (scalar string on every doc).
        var first = BuildSeed(400);
        await SeedAsync(store, first);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced
                .AsyncRawQuery<Item>("from index 'Items/Index' where Status = 'active' order by Status include timings()")
                .Timings(out var timings)
                .ToListAsync();

            var plan = timings.QueryPlan as QueryInspectionNode;
            Assert.NotNull(plan);
            Assert.True(SortElided(plan), "Phase 1: expected the sort to be elided (single-valued). Plan: " + Describe(plan));
            Assert.Equal(first.Count(i => i.Status == "active"), results.Count);
        }

        // Phase 2: store a doc whose Status is an array (same "Items" collection), making the Status
        // field multi-valued index-wide.
        using (var session = store.OpenAsyncSession())
        {
            var mv = new ItemMv { Id = "items/flip", Status = new[] { "active", "zzz" }, Score = 100000, Tags = new[] { "x", "y" } };
            await session.StoreAsync(mv);
            session.Advanced.GetMetadataFor(mv)["@collection"] = "Items";
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenAsyncSession())
        {
            // Deserialize into an Id-only DTO: the flip doc's Status is now a JSON array, which would not
            // bind to Item.Status (a string).
            var results = await session.Advanced
                .AsyncRawQuery<IdOnly>("from index 'Items/Index' where Status = 'active' order by Status include timings()")
                .Timings(out var timings)
                .ToListAsync();

            var plan = timings.QueryPlan as QueryInspectionNode;
            Assert.NotNull(plan);
            // The single-valued bit changed -> cache miss re-plans -> the sort must NOT be elided now.
            Assert.True(SortApplied(plan),
                "After the field became multi-valued the sort must be re-applied. Plan: " + Describe(plan));

            // All 'active' docs (including the new array doc) must be present.
            Assert.Equal(first.Count(i => i.Status == "active") + 1, results.Count);
            Assert.Contains(results, r => r.Id == "items/flip");
        }
    }

    // The single->multi flip for the residual straight-line IL. Code starts single-valued (a Code='common'
    // residual compiles to a single read + compare). Adding a doc whose Code is ["aaa","common"] makes the
    // field multi-valued; since 'aaa' < 'common', a stale single-read plan would read only the first term and
    // drop the doc. The cache-key fold forces a re-plan to the looping IL, which walks both terms and matches.
    // The dangerous case: a wrong plan here silently returns incomplete results rather than throwing.
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task ResidualScan_FieldFlipsToMultiValued_StillMatchesViaSecondTerm()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        new Widget_Index().Execute(store);

        // Sized like DirectScanResidualTests so the residual must be NON-selective: 'common' on 3/4 of docs
        // means the planner seeds on the ORDER BY Score sorted scan and Code='common' becomes a residual
        // (the straight-line IL path under test) rather than a selective postings seed. Scores are spaced so
        // the flip doc (Score 5) lands deterministically inside the top-25 window.
        var docs = new List<Widget>(4000);
        for (int i = 0; i < 4000; i++)
            docs.Add(new Widget { Id = $"widgets/{i}", Code = (i % 4 == 3) ? $"u{i}" : "common", Score = i * 10 });
        using (var bulk = store.BulkInsert())
            foreach (var d in docs)
                await bulk.StoreAsync(d, d.Id);
        Indexes.WaitForIndexing(store);

        const string query = "from index 'Widget/Index' where Score between 0 and 40000 and Code = 'common' order by Score as long limit 25 include timings()";

        // Phase 1: Code is single-valued -> straight-line residual IL.
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<Widget>(query)
                .Timings(out var timings)
                .ToListAsync();

            var plan = timings.QueryPlan as QueryInspectionNode;
            Assert.NotNull(plan);
            // A sorted scan (not the bitmap pipeline) confirms the residual straight-line IL is the path under test.
            Assert.True(Hint(plan) != "BitmapPipeline", "Phase 1 should use a residual scan. Plan: " + Describe(plan));

            var expected = docs.Where(d => d.Code == "common").OrderBy(d => d.Score).Take(25).Select(d => d.Id).ToList();
            Assert.Equal(expected, results.Select(r => r.Id).ToList());
        }

        // Phase 2: add a multi-valued doc; 'aaa' < 'common', so a stale single-read plan would drop it.
        using (var session = store.OpenAsyncSession())
        {
            var mv = new WidgetMv { Id = "widgets/flip", Code = new[] { "aaa", "common" }, Score = 5 };
            await session.StoreAsync(mv);
            session.Advanced.GetMetadataFor(mv)["@collection"] = "Widgets";
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // Phase 3: same query -> cache miss (Code is now multi-valued) -> looping IL -> the flip doc matches.
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<IdOnly>(query).ToListAsync();
            var ids = results.Select(r => r.Id).ToList();

            Assert.Contains("widgets/flip", ids);

            // Full top-25 expectation across the flipped set ('common' docs + the new doc at Score 5).
            var combined = docs.Where(d => d.Code == "common").Select(d => (d.Id, d.Score)).ToList();
            combined.Add(("widgets/flip", 5));
            var expected = combined.OrderBy(x => x.Score).Take(25).Select(x => x.Id).ToList();
            Assert.Equal(expected, ids);
        }
    }
}
