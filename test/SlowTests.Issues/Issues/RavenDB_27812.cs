using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27812 : RavenTestBase
{
    public RavenDB_27812(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void OpenBetweenMustOnlyMatchEntriesWithTheField(Options options)
    {
        using var store = SeedAges(options);

        using var querySession = store.OpenSession();
        var results = querySession.Advanced.DocumentQuery<Item, Items_ByAge>().WhereBetween(x => x.Age, null, null).ToList();

        Assert.Equal(new int?[] { 1 }, results.Select(x => x.Age));
        Assert.Equal(1, Count<Item>(store, "from index 'Items/ByAge' where Age between '*' and 'NULL' or Age = 5"));
        Assert.Equal(0, Count<Item>(store, "from index 'Items/ByAge' where Other >= 0 and Age between '*' and 'NULL' order by Other as long", ("rvn_corax_strategy", "FieldSortedScan")));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void NegatedOpenBetweenMustOnlyMatchEntriesWithoutTheField(Options options)
    {
        using var store = SeedAges(options);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Item>("from index 'Items/ByAge' where true and not Age between '*' and 'NULL'").NoCaching().ToList();

        Assert.Equal(new int?[] { null }, results.Select(x => x.Age));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void BoostedOpenBetweenMustOnlyMatchEntriesWithTheField(Options options)
    {
        using var store = SeedAges(options);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Item>("from index 'Items/ByAge' where boost(Age between '*' and 'NULL', 2)").NoCaching().ToList();

        Assert.Equal(new int?[] { 1 }, results.Select(x => x.Age));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void OpenBetweenMustMatchNullValues(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Item { Age = 1 });
            session.Store(new Item { Age = null });
            session.SaveChanges();
        }

        new Items_ByAgeStatic().Execute(store);
        Indexes.WaitForIndexing(store);

        Assert.Equal(2, Count<Item>(store, "from index 'Items/ByAgeStatic' where Age between '*' and 'NULL'"));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void OpenRangeBetweenAfterClosedBetween(Options options)
    {
        using var store = SeedNames(options);

        using var session = store.OpenSession();
        Assert.Equal(11, session.Advanced.RawQuery<Item2>("from index 'Items/Index' where Name between 'n010' and 'n020'").ToList().Count);
        Assert.Equal(21, session.Advanced.RawQuery<Item2>("from index 'Items/Index' where Name between '*' and 'n020'").ToList().Count);
        Assert.Equal(50, session.Advanced.RawQuery<Item2>("from index 'Items/Index' where Name between 'n150' and 'NULL'").ToList().Count);
    }

    private static readonly (string Low, string High, int Expected)[] OpenAndClosedRanges =
    [
        ("*", "n020", 21),
        ("n010", "n020", 11),
        ("n150", "NULL", 50),
        ("*", "n020", 21),
        ("n150", "n160", 11),
        ("*", "NULL", 200),
        ("n010", "n020", 11)
    ];

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void OpenAndClosedLiteralBetweenInAnyOrder(Options options)
    {
        using var store = SeedNames(options);

        foreach (var (low, high, expected) in OpenAndClosedRanges)
            Assert.Equal(expected, Count<Item2>(store, $"from index 'Items/Index' where Name between '{low}' and '{high}'"));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void OpenAndClosedParameterBetweenInAnyOrder(Options options)
    {
        using var store = SeedNames(options);

        foreach (var (low, high, expected) in OpenAndClosedRanges)
            Assert.Equal(expected, Count<Item2>(store, "from index 'Items/Index' where Name between $low and $high", ("low", low), ("high", high)));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void OpenAndClosedBetweenOnDates(Options options)
    {
        using var store = GetDocumentStore(options);
        var start = new DateTime(2019, 1, 1);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 200; i++)
                bulk.Store(new Event { At = start.AddDays(i) });
        }

        new Events_ByAt().Execute(store);
        Indexes.WaitForIndexing(store);

        Assert.Equal(11, Count<Event>(store, "from index 'Events/ByAt' where At between '2019-01-11T00:00:00.0000000' and '2019-01-21T00:00:00.0000000'"));
        Assert.Equal(21, Count<Event>(store, "from index 'Events/ByAt' where At between '*' and '2019-01-21T00:00:00.0000000'"));
        Assert.Equal(20, Count<Event>(store, "from index 'Events/ByAt' where At between '2019-06-30T00:00:00.0000000' and 'NULL'"));
    }

    // the cheaper clause goes first, so the open BETWEEN moves between the two positions of the cached plan
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void TwoBetweensThatSwapPlaces(Options options)
    {
        using var store = SeedNameTags(options);

        const string rql = "from index 'Docs/ByNameTag' where Name between $a and $b and Tag between $c and $d";
        Assert.Equal(21, Count<Doc>(store, rql, ("a", "*"), ("b", "n020"), ("c", "m000"), ("d", "m150")));
        Assert.Equal(11, Count<Doc>(store, rql, ("a", "*"), ("b", "n180"), ("c", "m000"), ("d", "m010")));
        Assert.Equal(21, Count<Doc>(store, rql, ("a", "*"), ("b", "n020"), ("c", "m000"), ("d", "m150")));
        Assert.Equal(21, Count<Doc>(store, rql, ("a", "n000"), ("b", "n150"), ("c", "*"), ("d", "m020")));

        Assert.Equal(21, Count<Doc>(store, "from index 'Docs/ByNameTag' where Name between '*' and 'n020' and Tag between 'm000' and 'm150'"));
        Assert.Equal(11, Count<Doc>(store, "from index 'Docs/ByNameTag' where Name between '*' and 'n180' and Tag between 'm000' and 'm010'"));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void OpenBetweenAndRangeThatSwapPlaces(Options options)
    {
        using var store = SeedNameTags(options);

        Assert.Equal(10, Count<Doc>(store, "from index 'Docs/ByNameTag' where Name >= 'n190' and Tag between 'm050' and 'NULL'"));
        Assert.Equal(10, Count<Doc>(store, "from index 'Docs/ByNameTag' where Name >= 'n050' and Tag between 'm190' and 'NULL'"));
        Assert.Equal(10, Count<Doc>(store, "from index 'Docs/ByNameTag' where Name >= 'n190' and Tag between 'm050' and 'NULL'"));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void OpenAndClosedBetweenAsSortedScanFilter(Options options)
    {
        using var store = SeedNameTags(options);

        foreach (var (low, high, expected) in new[] { ("m010", "m020", 11), ("*", "m020", 21), ("m180", "NULL", 20), ("*", "NULL", 200), ("m010", "m020", 11) })
        {
            var rql = $"from index 'Docs/ByNameTag' where Name >= 'n000' and Tag between '{low}' and '{high}' order by Name";
            Assert.Equal(expected, Count<Doc>(store, rql, ("rvn_corax_strategy", "FieldSortedScan")));
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void OpenBetweenOnTheSortField(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Thing { Name = "a" });
            session.Store(new Thing { Name = "b" });
            session.Store(new Thing { Name = null });
            session.Store(new Thing { Name = "c", Missing = true });
            session.SaveChanges();
        }

        new Things_ByName().Execute(store);
        Indexes.WaitForIndexing(store);

        foreach (var forced in new[] { null, "FieldSortedScan" })
        {
            using var session = store.OpenSession();
            var query = session.Advanced.RawQuery<Thing>("from index 'Things/ByName' where Name between '*' and 'NULL' order by Name").NoCaching();
            if (forced != null)
                query = query.AddParameter("rvn_corax_strategy", forced);

            var names = query.ToList().Select(x => x.Name).OrderBy(x => x, StringComparer.Ordinal);
            Assert.Equal(new[] { null, "a", "b" }, names);
        }
    }

    private static int Count<T>(IDocumentStore store, string rql, params (string Name, object Value)[] parameters)
    {
        using var session = store.OpenSession();
        var query = session.Advanced.RawQuery<T>(rql).NoCaching();
        foreach (var (name, value) in parameters)
            query = query.AddParameter(name, value);
        return query.ToList().Count;
    }

    private IDocumentStore SeedAges(Options options)
    {
        var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Item { Age = 1 });
            session.Store(new Item { Age = null });
            session.SaveChanges();
        }

        new Items_ByAge().Execute(store);
        Indexes.WaitForIndexing(store);
        return store;
    }

    private IDocumentStore SeedNames(Options options)
    {
        var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 200; i++)
                bulk.Store(new Item2 { Name = $"n{i:D3}" });
        }

        new Items_Index().Execute(store);
        Indexes.WaitForIndexing(store);
        return store;
    }

    private IDocumentStore SeedNameTags(Options options)
    {
        var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 200; i++)
                bulk.Store(new Doc { Name = $"n{i:D3}", Tag = $"m{i:D3}" });
        }

        new Docs_ByNameTag().Execute(store);
        Indexes.WaitForIndexing(store);
        return store;
    }

    private class Item
    {
        public string Id { get; set; }
        public int? Age { get; set; }
    }

    // the map leaves Age out of the entry when the document has none
    private class Items_ByAge : AbstractJavaScriptIndexCreationTask
    {
        public Items_ByAge()
        {
            Maps = new HashSet<string>
            {
                "map('Items', function (i) { return i.Age == null ? { Other: 1 } : { Age: i.Age }; })"
            };
        }
    }

    private class Items_ByAgeStatic : AbstractIndexCreationTask<Item>
    {
        public Items_ByAgeStatic()
        {
            Map = items => from i in items
                select new { i.Age };
        }
    }

    private class Item2
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class Items_Index : AbstractIndexCreationTask<Item2>
    {
        public Items_Index()
        {
            Map = items => from i in items
                select new { i.Name };
        }
    }

    private class Doc
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Tag { get; set; }
    }

    private class Docs_ByNameTag : AbstractIndexCreationTask<Doc>
    {
        public Docs_ByNameTag()
        {
            Map = docs => from d in docs
                select new { d.Name, d.Tag };
        }
    }

    private class Event
    {
        public string Id { get; set; }
        public DateTime At { get; set; }
    }

    private class Events_ByAt : AbstractIndexCreationTask<Event>
    {
        public Events_ByAt()
        {
            Map = events => from e in events
                select new { e.At };
        }
    }

    private class Thing
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public bool Missing { get; set; }
    }

    // a null Name is indexed as null, a Missing one is left out of the entry
    private class Things_ByName : AbstractJavaScriptIndexCreationTask
    {
        public Things_ByName()
        {
            Maps = new HashSet<string>
            {
                "map('Things', function (t) { return t.Missing ? { Other: 1 } : { Name: t.Name }; })"
            };
        }
    }
}
