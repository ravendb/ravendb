using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27810 : RavenTestBase
{
    public RavenDB_27810(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void NegatedRangeMustNotTakeOverTheSortedScan(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 100; i++)
                bulk.Store(new Item { Name = $"n{i:D3}", Age = i });
        }

        new Items_Index().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();

        var byAge = session.Advanced.DocumentQuery<Item, Items_Index>()
            .WhereGreaterThan(x => x.Age, 10)
            .AndAlso()
            .Not.WhereGreaterThan(x => x.Age, 50)
            .OrderBy(x => x.Age, OrderingType.Long)
            .Take(5)
            .ToList();
        Assert.Equal(new long[] { 11, 12, 13, 14, 15 }, byAge.Select(x => x.Age));

        var byName = session.Advanced.DocumentQuery<Item, Items_Index>()
            .WhereGreaterThan(x => x.Name, "n050")
            .AndAlso()
            .Not.WhereBetween(x => x.Name, "n060", "n070")
            .OrderBy(x => x.Name)
            .Take(5)
            .ToList();
        Assert.Equal(new[] { "n051", "n052", "n053", "n054", "n055" }, byName.Select(x => x.Name));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void LeadingSpatialClauseMustNotChangeTheDrivingClause(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            // names run opposite to insertion order, every document is inside the circle
            for (int i = 0; i < 200; i++)
                bulk.Store(new Item2 { Name = $"n{199 - i:D3}", Tag = (i % 10).ToString(), Lat = 0, Lng = 0 });
        }

        new Items_Index2().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();
        var results = session.Query<Item2, Items_Index2>()
            .Spatial("Location", f => f.WithinRadius(100, 0, 0))
            .Where(x => x.Tag == "4" && x.Name.CompareTo("n050") > 0)
            .OrderBy(x => x.Name)
            .Take(3)
            .ToList();

        Assert.Equal(new[] { "n055", "n065", "n075" }, results.Select(x => x.Name));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void SortedScansMustNotDropFilters(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            // every third document is outside the circle
            for (int i = 0; i < 200; i++)
                bulk.Store(new Item3 { Name = $"n{i:D3}", Tag = (i % 10).ToString(), Age = i, Lat = i % 3 == 0 ? 50 : 0, Lng = 0 });
        }

        new Items_Index3().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();

        var fieldScan = session.Query<Item3, Items_Index3>()
            .Where(x => x.Name.CompareTo("n050") > 0)
            .Spatial("Location", f => f.WithinRadius(100, 0, 0))
            .OrderBy(x => x.Name)
            .Take(5)
            .ToList();
        Assert.Equal(new[] { "n052", "n053", "n055", "n056", "n058" }, fieldScan.Select(x => x.Name));

        var compoundScan = session.Query<Item3, Items_Index3>()
            .Where(x => x.Tag == "4")
            .Spatial("Location", f => f.WithinRadius(100, 0, 0))
            .OrderBy(x => x.Age)
            .Take(5)
            .ToList();
        Assert.Equal(new long[] { 4, 14, 34, 44, 64 }, compoundScan.Select(x => x.Age));

        var compoundLookup = session.Query<Item3, Items_Index3>()
            .Where(x => x.Tag == "3" && x.Age == 3)
            .Spatial("Location", f => f.WithinRadius(100, 0, 0))
            .ToList();
        Assert.Empty(compoundLookup);

        var negatedField2 = session.Advanced.DocumentQuery<Item3, Items_Index3>()
            .WhereEquals(x => x.Tag, "4")
            .AndAlso()
            .Not.WhereGreaterThan(x => x.Age, 150)
            .OrderBy(x => x.Age, OrderingType.Long)
            .Take(3)
            .ToList();
        Assert.Equal(new long[] { 4, 14, 24 }, negatedField2.Select(x => x.Age));
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public long Age { get; set; }
    }

    private class Items_Index : AbstractIndexCreationTask<Item>
    {
        public Items_Index()
        {
            Map = items => from i in items
                select new { i.Name, i.Age };
        }
    }

    private class Item2
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Tag { get; set; }
        public double Lat { get; set; }
        public double Lng { get; set; }
    }

    private class Items_Index2 : AbstractIndexCreationTask<Item2>
    {
        public Items_Index2()
        {
            Map = items => from i in items
                select new { i.Name, i.Tag, Location = CreateSpatialField(i.Lat, i.Lng) };
        }
    }

    private class Item3
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Tag { get; set; }
        public long Age { get; set; }
        public double Lat { get; set; }
        public double Lng { get; set; }
    }

    private class Items_Index3 : AbstractIndexCreationTask<Item3>
    {
        public Items_Index3()
        {
            Map = items => from i in items
                select new { i.Name, i.Tag, i.Age, Location = CreateSpatialField(i.Lat, i.Lng) };
            CompoundField(x => x.Tag, x => x.Age);
        }
    }
}
