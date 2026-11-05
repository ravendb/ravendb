using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Queries.Facets;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

// The loops that read the terms of many entries share one CompactKey: results and memory must not depend on it.
[Collection(CompactKeySharingQueriesCollection.Name)]
public class CompactKeySharingQueries(ITestOutputHelper output) : RavenTestBase(output)
{
    private const int Entries = 4_000;
    private static readonly string[] Colors = ["red", "green", "blue", "yellow"];

    private DocumentStore CreateStore()
    {
        var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        new ItemsIndex().Execute(store);
        return store;
    }

    private static List<Item> Data() => Enumerable.Range(0, Entries).Select(i => new Item
    {
        Id = $"items/{i}",
        Color = Colors[i % Colors.Length],
        Price = i,
        Text = i % 10 == 0 ? $"the quick brown fox number{i}" : i % 10 == 1 ? $"brown quick fox number{i}" : $"lazy dog number{i}",
        Latitude = 50 + (i % 100) * 0.05,
        Longitude = 3 + (i / 100) * 0.05,
    }).ToList();

    private void Insert(DocumentStore store, IEnumerable<Item> items)
    {
        using (var bulk = store.BulkInsert())
        {
            foreach (var item in items)
                bulk.Store(item, item.Id);
        }

        Indexes.WaitForIndexing(store);
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void PhraseQueryReturnsOnlyEntriesWithTheWordsInOrder()
    {
        using var store = CreateStore();
        var data = Data();
        Insert(store, data);

        using var session = store.OpenSession();
        var results = session.Query<Item, ItemsIndex>().Search(x => x.Text, "\"quick brown\"").ToList();
        Assert.Equal(data.Count(x => x.Text.StartsWith("the quick brown")), results.Count);
        Assert.All(results, x => Assert.Contains("quick brown", x.Text));

        var reversed = session.Query<Item, ItemsIndex>().Search(x => x.Text, "\"brown quick\"").ToList();
        Assert.Equal(data.Count(x => x.Text.StartsWith("brown quick")), reversed.Count);
        Assert.All(reversed, x => Assert.StartsWith("brown quick", x.Text));

        Assert.Empty(session.Query<Item, ItemsIndex>().Search(x => x.Text, "\"dog lazy\"").ToList());
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void MultiUnaryQueryReturnsTheEntriesThatMatchEveryCondition()
    {
        using var store = CreateStore();
        var data = Data();
        Insert(store, data);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Item>("from index 'ItemsIndex' where Color = 'red' and Price > 1000 and Price < 3000 and Price != 1400 and Price >= 1004")
            .ToList();

        var expected = data.Where(x => x.Color == "red" && x.Price > 1000 && x.Price < 3000 && x.Price != 1400 && x.Price >= 1004).Select(x => x.Id).OrderBy(x => x).ToList();
        Assert.NotEmpty(expected);
        Assert.Equal(expected, results.Select(x => x.Id).OrderBy(x => x).ToList());
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Spatial)]
    public void SpatialQueryReturnsTheEntriesWithinTheRadius()
    {
        using var store = CreateStore();
        var data = Data();
        Insert(store, data);

        using var session = store.OpenSession();
        var results = session.Query<Item, ItemsIndex>().Spatial("Coordinates", f => f.WithinRadius(30, 51, 4)).ToList();

        Assert.NotEmpty(results);
        Assert.True(results.Count < data.Count);
        var farthest = results.Max(x => HaversineKm(x.Latitude, x.Longitude, 51, 4));
        Assert.True(farthest < 30.5, $"farthest returned {farthest}");

        // nothing within the radius is missing (margin for the geohash precision)
        var missing = data.Where(x => HaversineKm(x.Latitude, x.Longitude, 51, 4) < 29.5).Select(x => x.Id).Except(results.Select(x => x.Id)).ToList();
        Assert.Empty(missing);
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Facets)]
    public void FacetsCountEveryEntry()
    {
        using var store = CreateStore();
        var data = Data();
        Insert(store, data);

        using var session = store.OpenSession();
        var facets = session.Query<Item, ItemsIndex>()
            .Where(x => x.Price >= 1000)
            .AggregateBy(f => f.ByField(x => x.Color).SumOn(x => x.Price))
            .Execute();

        var values = facets["Color"].Values.ToDictionary(x => x.Range, x => x);
        foreach (var color in Colors)
        {
            var expected = data.Where(x => x.Price >= 1000 && x.Color == color).ToList();
            Assert.Equal(expected.Count, values[color].Count);
            Assert.Equal(expected.Sum(x => x.Price), values[color].Sum);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Facets)]
    public void RangeFacetsCountEveryEntry()
    {
        using var store = CreateStore();
        var data = Data();
        Insert(store, data);

        using var session = store.OpenSession();
        var facets = session.Query<Item, ItemsIndex>()
            .AggregateBy(f => f.ByRanges(x => x.Price < 1000, x => x.Price >= 1000 && x.Price < 3000, x => x.Price >= 3000))
            .Execute();

        var counts = facets["Price"].Values.Select(x => (long)x.Count).ToArray();
        Assert.Equal(new long[] { 1000, 2000, 1000 }, counts);
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void RepeatedQueriesOfTheSameKindGiveTheSameResults()
    {
        using var store = CreateStore();
        var data = Data();
        Insert(store, data);

        var expected = data.Count(x => x.Text.StartsWith("the quick brown"));
        for (var i = 0; i < 20; i++)
        {
            using var session = store.OpenSession();
            Assert.Equal(expected, session.Query<Item, ItemsIndex>().Search(x => x.Text, "\"quick brown\"").Count());
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void ManagedAllocationsOfMultiUnaryAndPhraseQueriesDoNotGrowWithThreeKilobytesPerEntry()
    {
        using var store = CreateStore();
        var data = Data();
        Insert(store, data);

        // warm up
        // a repeated query would come from the query cache, so every run uses a different text
        RunMeasured(store, "from index 'ItemsIndex' where Color = 'red' and Price > 1 and Price < 100000 select Id limit 0, 2");
        RunMeasured(store, "from index 'ItemsIndex' where search(Text, '\"quick brown\"') select Id limit 0, 2");

        var multiUnary = RunMeasured(store, "from index 'ItemsIndex' where Color = 'red' and Price > 0 and Price < 100000 select Id limit 0, 1");
        var phrase = RunMeasured(store, "from index 'ItemsIndex' where search(Text, '\"quick brown\"') select Id limit 0, 1");

        output.WriteLine($"multi unary: {multiUnary:N0} bytes for {Entries:N0} entries ({multiUnary / (double)Entries:F0} per entry)");
        output.WriteLine($"phrase: {phrase:N0} bytes for {Entries:N0} entries ({phrase / (double)Entries:F0} per entry)");

        // a shared key allocates little per entry
        Assert.True(multiUnary < Entries * 400L, $"multi unary allocated {multiUnary:N0} bytes");
        Assert.True(phrase < Entries * 400L, $"phrase allocated {phrase:N0} bytes");
    }

    // The query runs on a server thread of this process, so the allocations of every thread are counted.
    private static long RunMeasured(DocumentStore store, string rql)
    {
        using var session = store.OpenSession();
        var before = GC.GetTotalAllocatedBytes(precise: true);
        session.Advanced.RawQuery<Item>(rql).ToList();
        return GC.GetTotalAllocatedBytes(precise: true) - before;
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    public void UpdatesAndDeletesInOneBatchLeaveTheIndexConsistent()
    {
        using var store = CreateStore();
        var data = Data();
        Insert(store, data);

        // deleted and updated in one go, so that the index writer reads the terms of all of them
        using (var session = store.OpenSession())
        {
            session.Advanced.MaxNumberOfRequestsPerSession = 10_000;
            foreach (var item in data.Where(x => x.Price % 4 == 0))
                session.Delete(item.Id);

            foreach (var item in data.Where(x => x.Price % 4 == 1))
            {
                var loaded = session.Load<Item>(item.Id);
                loaded.Color = "purple";
                loaded.Price += 100_000;
            }

            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            Assert.Equal(Entries - Entries / 4, session.Query<Item, ItemsIndex>().Count());
            Assert.Equal(Entries / 4, session.Query<Item, ItemsIndex>().Count(x => x.Color == "purple"));
            Assert.Equal(Entries / 4, session.Query<Item, ItemsIndex>().Count(x => x.Price >= 100_000));
            Assert.Equal(0, session.Query<Item, ItemsIndex>().Count(x => x.Price == 4));
            Assert.Equal(0, session.Query<Item, ItemsIndex>().Count(x => x.Price == 1));
            Assert.Equal(1, session.Query<Item, ItemsIndex>().Count(x => x.Price == 100_001));

            // the old terms of the deleted/updated entries are gone from the other fields too
            var red = data.Count(x => x.Color == "red" && x.Price % 4 != 0 && x.Price % 4 != 1);
            Assert.Equal(red, session.Query<Item, ItemsIndex>().Count(x => x.Color == "red"));
            var phrase = data.Count(x => x.Text.StartsWith("the quick brown") && x.Price % 4 != 0);
            Assert.Equal(phrase, session.Query<Item, ItemsIndex>().Search(x => x.Text, "\"quick brown\"").Count());
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    public void IndexKeepsWorkingAfterManyBatchesOfDeletesAndReinserts()
    {
        using var store = CreateStore();
        var data = Data().Take(600).ToList();
        Insert(store, data);

        for (var round = 0; round < 5; round++)
        {
            using (var session = store.OpenSession())
            {
                foreach (var item in data.Where(x => x.Price % 5 == round))
                    session.Delete(item.Id);
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
            Insert(store, data.Where(x => x.Price % 5 == round));

            using var check = store.OpenSession();
            Assert.Equal(data.Count, check.Query<Item, ItemsIndex>().Count());
            Assert.Equal(data.Count(x => x.Color == "blue"), check.Query<Item, ItemsIndex>().Count(x => x.Color == "blue"));
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Vector)]
    public void VectorSearchWithAFilterReturnsOnlyEntriesOfTheFilter()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        var random = new Random(5);
        var vectors = new List<float[]>();
        using (var bulk = store.BulkInsert())
        {
            for (var i = 0; i < 300; i++)
            {
                var vector = Enumerable.Range(0, 16).Select(_ => (float)(random.NextDouble() * 2 - 1)).ToArray();
                vectors.Add(vector);
                bulk.Store(new VectorItem { Id = $"vectors/{i}", Category = i % 3 == 0 ? "a" : "b", Embedding = vector }, $"vectors/{i}");
            }
        }

        const string rql = "from 'VectorItems' where Category = 'a' and vector.search(Embedding, $p0, 0.5, 20)";
        using var session = store.OpenSession();
        var target = vectors[30]; // group "a"
        var results = session.Advanced.RawQuery<VectorItem>(rql).AddParameter("p0", target).WaitForNonStaleResults().ToList();

        Assert.NotEmpty(results);
        Assert.All(results, x => Assert.Equal("a", x.Category));
        Assert.Contains(results, x => x.Id == "vectors/30");

        // without a filter the other group is returned as well
        var unfiltered = session.Advanced.RawQuery<VectorItem>("from 'VectorItems' where vector.search(Embedding, $p0, 0.5, 20)").AddParameter("p0", target).ToList();
        Assert.Contains(unfiltered, x => x.Id == "vectors/30");
        Assert.True(unfiltered.Count >= results.Count);
    }

    private class VectorItem
    {
        public string Id { get; set; }
        public string Category { get; set; }
        public float[] Embedding { get; set; }
    }

    private static double HaversineKm(double lat1, double lng1, double lat2, double lng2)
    {
        var dLat = (lat2 - lat1) * Math.PI / 180;
        var dLng = (lng2 - lng1) * Math.PI / 180;
        var a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) + Math.Cos(lat1 * Math.PI / 180) * Math.Cos(lat2 * Math.PI / 180) * Math.Sin(dLng / 2) * Math.Sin(dLng / 2);
        return 2 * 6371.0087714 * Math.Asin(Math.Sqrt(a));
    }

    private class Item
    {
        public string Id { get; set; }
        public string Color { get; set; }
        public long Price { get; set; }
        public string Text { get; set; }
        public double Latitude { get; set; }
        public double Longitude { get; set; }
    }

    private class ItemsIndex : AbstractIndexCreationTask<Item>
    {
        public ItemsIndex()
        {
            Map = items => from i in items
                select new { i.Color, i.Price, i.Text, Coordinates = CreateSpatialField(i.Latitude, i.Longitude) };

            Index(x => x.Text, FieldIndexing.Search);
        }
    }
}

// the memory tests count the allocations of the whole process, so no other test may run meanwhile
[CollectionDefinition(Name, DisableParallelization = true)]
public class CompactKeySharingQueriesCollection
{
    public const string Name = nameof(CompactKeySharingQueriesCollection);
}
