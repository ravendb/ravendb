using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27803 : RavenTestBase
{
    public RavenDB_27803(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ResidualMustCheckEveryValueOfADynamicField(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 300; i++)
                bulk.Store(new Item { Tag = i < 2 ? "rare" : "t", Attrs = ["first" + (i % 3), "second" + (i % 5)] });
        }

        new Items_Index().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Item, Items_Index>()
            .WhereEquals(x => x.Tag, "rare")
            .AndAlso()
            .WhereGreaterThan("Attr", "s")
            .ToList();

        Assert.Equal(2, results.Count);
    }

    private class Item
    {
        public string Id { get; set; }
        public string Tag { get; set; }
        public string[] Attrs { get; set; }
    }

    private class Items_Index : AbstractIndexCreationTask<Item>
    {
        public Items_Index()
        {
            Map = items => from i in items
                select new { i.Tag, _ = i.Attrs.Select(a => CreateField("Attr", a)) };
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void StringValueMustNotSatisfyNumericRangeInEntryScan(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 300; i++)
                bulk.Store(new Item2 { Age = i, Tag = "t" });
            bulk.Store(new Item2 { Age = "abc", Tag = "rare" });
        }

        new Items_Index2().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Item2, Items_Index2>()
            .WhereEquals(x => x.Tag, "rare")
            .AndAlso()
            .WhereLessThan("Age", 250)
            .ToList();

        Assert.Empty(results);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void StringValueMustNotSatisfyNumericInInEntryScan(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 300; i++)
                bulk.Store(new Item2 { Age = i % 2, Tag = "t" });
            bulk.Store(new Item2 { Age = "abc", Tag = "rare" });
        }

        new Items_Index2().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Item2, Items_Index2>()
            .WhereEquals(x => x.Tag, "rare")
            .AndAlso()
            .WhereIn("Age", new object[] { 0, 1 })
            .ToList();

        Assert.Empty(results);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void NullOnlyInMustNotThrowInEntryScan(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 300; i++)
                bulk.Store(new Item2 { Age = i, Tag = "t" });
            bulk.Store(new Item2 { Age = 5, Tag = "rare" });
        }

        new Items_Index2().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Item2>("from index 'Items/Index2' where Tag = 'rare' and Age < 250 and not Age in (null)").ToList();

        Assert.Single(results);
    }

    private class Item2
    {
        public string Id { get; set; }
        public object Age { get; set; }
        public string Tag { get; set; }
    }

    private class Items_Index2 : AbstractIndexCreationTask<Item2>
    {
        public Items_Index2()
        {
            Map = items => from i in items
                select new { i.Age, i.Tag };
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ExistsMustNotMatchDocumentWithoutTheField(Options options)
    {
        options.ModifyDocumentStore = s => s.Conventions.FindCollectionName = _ => "Items";
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 300; i++)
                bulk.Store(new Item3 { Name = $"n{i:D3}", Tag = "t" });
            bulk.Store(new ItemWithoutName { Tag = "rare" });
        }

        new Items_Index3().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Item3, Items_Index3>()
            .WhereEquals(x => x.Tag, "rare")
            .AndAlso()
            .WhereExists(x => x.Name)
            .ToList();

        Assert.Empty(results);
    }

    private class Item3
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Tag { get; set; }
    }

    private class ItemWithoutName
    {
        public string Id { get; set; }
        public string Tag { get; set; }
    }

    private class Items_Index3 : AbstractIndexCreationTask<Item3>
    {
        public Items_Index3()
        {
            Map = items => from i in items
                select new { i.Name, i.Tag };
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void NullCheckMustSeeEveryValue(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            // `Tag = 'rare'` selects one entry, so `Tags = null` is checked per entry
            for (int i = 0; i < 300; i++)
                bulk.Store(new Item4 { Tag = i == 0 ? "rare" : "t", Tags = ["a", null], Name = null });
        }

        new Items_Index4().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();

        var withNull = session.Advanced.DocumentQuery<Item4, Items_Index4>().WhereEquals(x => x.Tag, "rare").AndAlso().WhereEquals("Tags", null).ToList();
        Assert.Single(withNull);

        var withoutNull = session.Advanced.DocumentQuery<Item4, Items_Index4>().WhereEquals(x => x.Tag, "rare").AndAlso().WhereNotEquals("Tags", null).ToList();
        Assert.Empty(withoutNull);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void AllInWithNullMustSeeTheNullMarker(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 300; i++)
                bulk.Store(new Item4 { Tag = i == 0 ? "rare" : "t", Tags = ["a", null] });
        }

        new Items_Index4().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();

        var withValue = session.Advanced.RawQuery<Item4>("from index 'Items/Index4' where Tag = 'rare' and Tags all in ('a', null)").ToList();
        Assert.Single(withValue);

        var onlyNull = session.Advanced.RawQuery<Item4>("from index 'Items/Index4' where Tag = 'rare' and Tags all in (null)").ToList();
        Assert.Single(onlyNull);
    }

    private class Item4
    {
        public string Id { get; set; }
        public string Tag { get; set; }
        public string[] Tags { get; set; }
        public string Name { get; set; }
    }

    private class Items_Index4 : AbstractIndexCreationTask<Item4>
    {
        public Items_Index4()
        {
            Map = items => from i in items
                select new { i.Tag, i.Tags, i.Name };
        }
    }
}
