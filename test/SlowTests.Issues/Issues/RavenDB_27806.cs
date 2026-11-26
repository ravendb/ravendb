using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Highlighting;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27806 : RavenTestBase
{
    public RavenDB_27806(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Spatial)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void SortedScanMustApplyTheSpatialFilter(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 200; i++)
                bulk.Store(new Item { Age = i, Lat = i % 2 == 0 ? 0 : 50, Lng = 0 });
        }

        new Items_BySpatial().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();
        var results = session.Query<Item, Items_BySpatial>()
            .Where(x => x.Age > 10)
            .Spatial("Location", f => f.WithinRadius(100, 0, 0))
            .OrderBy(x => x.Age)
            .Take(5)
            .ToList();

        Assert.Equal(new long[] { 12, 14, 16, 18, 20 }, results.Select(x => x.Age));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Highlighting)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void DirectScanMustKeepHighlights(Options options)
    {
        using var store = GetDocumentStore(options);
        Seed(store);

        using var session = store.OpenSession();
        var results = session.Query<Item, Items_ByText>()
            .Highlight(x => x.Text, 100, 1, out Highlightings highlightings)
            .Where(x => x.Text == "hello" && x.Age > 50)
            .OrderBy(x => x.Age)
            .Take(3)
            .ToList();

        Assert.Equal(new long[] { 52, 54, 56 }, results.Select(x => x.Age));
        Assert.All(results, x => Assert.Contains(highlightings.GetFragments(x.Id), f => f.Contains(">hello<")));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CompoundLookupMustBeSorted(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            for (int i = 0; i < 10; i++)
                session.Store(new Item { Tag = "t0", Name = "x0", Age = (i * 7) % 10 });
            session.SaveChanges();
        }

        new Items_ByTagAndName().Execute(store);
        Indexes.WaitForIndexing(store);

        using var querySession = store.OpenSession();
        var page = querySession.Query<Item, Items_ByTagAndName>()
            .Where(x => x.Tag == "t0" && x.Name == "x0")
            .OrderByDescending(x => x.Age)
            .Take(3)
            .ToList();

        Assert.Equal(new long[] { 9, 8, 7 }, page.Select(x => x.Age));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Highlighting)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TwoKeyWalkMustKeepHighlights(Options options)
    {
        using var store = GetDocumentStore(options);
        Seed(store);

        AssertHighlights<Items_ByText>(store, "FieldSortedScan", nameof(Item.Text), ">hello<", [52, 54, 56],
            q => q.WhereEquals(x => x.Text, "hello").AndAlso().WhereGreaterThan(x => x.Age, 50L).OrderBy(x => x.Age).OrderBy(x => x.Name).Take(3));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Highlighting)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CompoundSortedScanMustKeepHighlights(Options options)
    {
        using var store = GetDocumentStore(options);
        Seed(store);

        AssertHighlights<Items_ByTagAndAge>(store, "CompoundSortedScan", nameof(Item.Tag), ">t0<", [52, 54, 56],
            q => q.WhereEquals(x => x.Tag, "t0").AndAlso().WhereGreaterThan(x => x.Age, 50L).OrderBy(x => x.Age).Take(3));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Highlighting)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CompoundKeyLookupMustKeepHighlights(Options options)
    {
        using var store = GetDocumentStore(options);
        Seed(store);

        AssertHighlights<Items_ByTagAndName>(store, "CompoundKeyLookup", nameof(Item.Name), ">x0<", [196, 192, 188],
            q => q.WhereEquals(x => x.Tag, "t0").AndAlso().WhereEquals(x => x.Name, "x0").OrderByDescending(x => x.Age).Take(3));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Highlighting)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void NotEqualsOnTheWalkMustKeepHighlights(Options options)
    {
        using var store = GetDocumentStore(options);
        Seed(store);

        AssertHighlights<Items_ByText>(store, "FieldSortedScan", nameof(Item.Text), ">hello<", [52, 56, 60],
            q => q.WhereEquals(x => x.Text, "hello").AndAlso().WhereGreaterThan(x => x.Age, 50L).AndAlso().WhereNotEquals(x => x.Name, "x2").OrderBy(x => x.Age).Take(3));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Highlighting)]
    [RavenData("BitmapPipeline", SearchEngineMode = RavenSearchEngineMode.Corax)]
    [RavenData("FieldSortedScan", SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void InWithNullMustNotBreakHighlights(Options options, string strategy)
    {
        using var store = GetDocumentStore(options);
        Seed(store);

        AssertHighlights<Items_ByText>(store, strategy, nameof(Item.Text), ">hello<", [52, 56, 60],
            q => q.WhereEquals(x => x.Text, "hello").AndAlso().WhereGreaterThan(x => x.Age, 50L).AndAlso().WhereIn(x => x.Name, ["x0", null]).OrderBy(x => x.Age).Take(3));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Highlighting)]
    [RavenData("BitmapPipeline", SearchEngineMode = RavenSearchEngineMode.Corax)]
    [RavenData("FieldSortedScan", SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void OpenBetweenMustNotBreakHighlights(Options options, string strategy)
    {
        using var store = GetDocumentStore(options);
        Seed(store);

        AssertHighlights<Items_ByText>(store, strategy, nameof(Item.Text), ">hello<", [52, 54, 56],
            q => q.WhereEquals(x => x.Text, "hello").AndAlso().WhereGreaterThan(x => x.Age, 50L).AndAlso().WhereBetween(x => x.Name, "x0", null).OrderBy(x => x.Age).Take(3));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Highlighting)]
    [RavenData("BitmapPipeline", SearchEngineMode = RavenSearchEngineMode.Corax)]
    [RavenData("FieldSortedScan", SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void ExistsMustNotBreakHighlights(Options options, string strategy)
    {
        using var store = GetDocumentStore(options);
        Seed(store);

        AssertHighlights<Items_ByText>(store, strategy, nameof(Item.Text), null, [51, 52, 53],
            q => q.WhereExists(x => x.Text).AndAlso().WhereGreaterThan(x => x.Age, 50L).OrderBy(x => x.Age).Take(3));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Highlighting)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void SpatialOnlyQueryMustNotBreakHighlights(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            for (int i = 0; i < 3; i++)
                session.Store(new Item { Location = "POINT (0 0)" });
            session.SaveChanges();
        }

        new Items_ByLocation().Execute(store);
        Indexes.WaitForIndexing(store);

        using var querySession = store.OpenSession();
        var results = querySession.Advanced.DocumentQuery<Item, Items_ByLocation>()
            .Highlight(nameof(Item.Location), 100, 1, out Highlightings highlightings)
            .Spatial(nameof(Item.Location), f => f.WithinRadius(100, 0, 0))
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.All(results, x => Assert.Empty(highlightings.GetFragments(x.Id)));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Highlighting)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void EmptyInMustNotHighlightTheValueOfAnotherClause(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Item { Text = "hello", Name = "hello", Age = 1 });
            session.SaveChanges();
        }

        new Items_ByText().Execute(store);
        Indexes.WaitForIndexing(store);

        AssertHighlights<Items_ByText>(store, "BitmapPipeline", nameof(Item.Name), null, [1],
            q => q.WhereIn(x => x.Name, []).OrElse().WhereEquals(x => x.Text, "hello"));
    }

    private static void AssertHighlights<TIndex>(IDocumentStore store, string strategy, string field, string expectedHighlight, long[] expectedAges,
        Func<IDocumentQuery<Item>, IDocumentQuery<Item>> buildQuery) where TIndex : AbstractCommonApiForIndexes, new()
    {
        using var session = store.OpenSession();
        var query = session.Advanced.DocumentQuery<Item, TIndex>()
            .NoCaching()
            .Timings(out QueryTimings timings)
            .Highlight(field, 100, 1, out Highlightings highlightings)
            .AddParameter("rvn_corax_strategy", strategy);

        var results = buildQuery(query).ToList();

        string hint = null;
        Find((QueryInspectionNode)timings.QueryPlan, "CompiledQuery")?.Parameters?.TryGetValue("OptimizationHint", out hint);
        Assert.Equal(strategy, hint);
        Assert.Equal(expectedAges, results.Select(x => x.Age));
        foreach (var item in results)
        {
            var fragments = highlightings.GetFragments(item.Id);
            if (expectedHighlight == null)
                Assert.Empty(fragments);
            else
                Assert.Contains(fragments, f => f.Contains(expectedHighlight));
        }
    }

    private static QueryInspectionNode Find(QueryInspectionNode node, string operation)
    {
        if (node == null || node.Operation == operation)
            return node;
        return node.Children?.Select(c => Find(c, operation)).FirstOrDefault(x => x != null);
    }

    private void Seed(IDocumentStore store)
    {
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 200; i++)
                bulk.Store(new Item { Text = i % 2 == 0 ? $"hello world {i}" : $"goodbye moon {i}", Age = i, Tag = $"t{i % 2}", Name = $"x{i % 4}" });
        }

        new Items_ByText().Execute(store);
        new Items_ByTagAndAge().Execute(store);
        new Items_ByTagAndName().Execute(store);
        Indexes.WaitForIndexing(store);
    }

    private class Item
    {
        public string Id { get; set; }
        public string Text { get; set; }
        public string Tag { get; set; }
        public string Name { get; set; }
        public long Age { get; set; }
        public double Lat { get; set; }
        public double Lng { get; set; }
        public string Location { get; set; }
    }

    private class Items_ByLocation : AbstractIndexCreationTask<Item>
    {
        public Items_ByLocation()
        {
            Map = items => from i in items
                select new { Location = CreateSpatialField(i.Location) };
        }
    }

    private class Items_BySpatial : AbstractIndexCreationTask<Item>
    {
        public Items_BySpatial()
        {
            Map = items => from i in items
                select new { i.Age, Location = CreateSpatialField(i.Lat, i.Lng) };
        }
    }

    private class Items_ByText : AbstractIndexCreationTask<Item>
    {
        public Items_ByText()
        {
            Map = items => from i in items
                select new { i.Text, i.Age, i.Name };
            Store(x => x.Text, FieldStorage.Yes);
            Index(x => x.Text, FieldIndexing.Search);
            TermVector(x => x.Text, FieldTermVector.WithPositionsAndOffsets);
        }
    }

    private class Items_ByTagAndAge : AbstractIndexCreationTask<Item>
    {
        public Items_ByTagAndAge()
        {
            Map = items => from i in items
                select new { i.Tag, i.Age };
            CompoundField(x => x.Tag, x => x.Age);
        }
    }

    private class Items_ByTagAndName : AbstractIndexCreationTask<Item>
    {
        public Items_ByTagAndName()
        {
            Map = items => from i in items
                select new { i.Tag, i.Name, i.Age };
            CompoundField(x => x.Tag, x => x.Name);
        }
    }
}
