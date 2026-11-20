using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Highlighting;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Session;
using Raven.Server.Extensions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27802 : RavenTestBase
{
    public RavenDB_27802(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void LimitMustNotSkipTheRemainingClauses(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 200; i++)
                bulk.Store(new Item { Tag = i < 20 ? "rare" : "t", Age = i < 10 ? 1 : 100 });
        }

        new Items_Index().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();

        var withIn = session.Query<Item, Items_Index>().Where(x => x.Tag.In("rare", "x") && x.Age > 50).Take(3).ToList();
        Assert.Equal(new long[] { 100, 100, 100 }, withIn.Select(x => x.Age));

        var withOr = session.Query<Item, Items_Index>().Where(x => (x.Tag == "rare" || x.Tag == "x") && x.Age > 50).Take(3).ToList();
        Assert.Equal(new long[] { 100, 100, 100 }, withOr.Select(x => x.Age));
    }

    private class Item
    {
        public string Id { get; set; }
        public string Tag { get; set; }
        public long Age { get; set; }
    }

    private class Items_Index : AbstractIndexCreationTask<Item>
    {
        public Items_Index()
        {
            Map = items => from i in items
                select new { i.Tag, i.Age };
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void LimitMustApplyToTheResultNotToAnOperand(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 200; i++)
                bulk.Store(new Item2 { Name = $"n{i:D3}", Age = i % 30, Tag = i < 2 ? "rare" : "t" });
        }

        new Items_Index2().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();
        var results = session.Query<Item2, Items_Index2>()
            .Where(x => x.Name == "n000" || (x.Tag == "t" && x.Age == 7))
            .Take(10)
            .ToList();

        Assert.Equal(8, results.Count);
    }

    private class Item2
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public long Age { get; set; }
        public string Tag { get; set; }
    }

    private class Items_Index2 : AbstractIndexCreationTask<Item2>
    {
        public Items_Index2()
        {
            Map = items => from i in items
                select new { i.Name, i.Age, i.Tag };
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(false, SearchEngineMode = RavenSearchEngineMode.All)]
    [RavenData(true, SearchEngineMode = RavenSearchEngineMode.All)]
    public void PagesMustCoverTheWholeResultExactlyOnce(Options options, bool useIn)
    {
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 200; i++)
                bulk.Store(new Item3 { Age = (i * 37) % 200, Tag = i % 3 == 0 ? "a" : "b" });
        }

        new Items_Index3().Execute(store);
        Indexes.WaitForIndexing(store);

        Expression<Func<Item3, bool>> filter = useIn
            ? x => x.Tag.In("b", "a")
            : x => x.Tag == "a" || x.Age < 50;

        AssertPagesCoverTheWholeResult(store, filter, pageSize: 10);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void PagesOverARangeOfManyTermsMustCoverTheWholeResultExactlyOnce(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            // more distinct values than one batch of terms, in an order unrelated to the entry ids
            for (int i = 0; i < 5000; i++)
                bulk.Store(new Item3 { Age = (i * 37) % 5000, Tag = "a" });
        }

        new Items_Index3().Execute(store);
        Indexes.WaitForIndexing(store);

        AssertPagesCoverTheWholeResult(store, x => x.Age < 4500, pageSize: 500);
    }

    private static void AssertPagesCoverTheWholeResult(IDocumentStore store, Expression<Func<Item3, bool>> filter, int pageSize)
    {
        using var session = store.OpenSession();
        var all = session.Query<Item3, Items_Index3>().Where(filter).ToList().Select(x => x.Id).ToList();

        var paged = new List<string>();
        int pageNumber = 0;
        List<Item3> page;
        do
        {
            page = session.Query<Item3, Items_Index3>()
                .Where(filter)
                .Skip(pageNumber * pageSize)
                .Take(pageSize)
                .ToList();

            paged.AddRange(page.Select(x => x.Id));
            pageNumber++;
        } while (page.Count > 0);

        Assert.Equal(all.OrderBy(x => x), paged.OrderBy(x => x));
    }

    private class Item3
    {
        public string Id { get; set; }
        public long Age { get; set; }
        public string Tag { get; set; }
    }

    private class Items_Index3 : AbstractIndexCreationTask<Item3>
    {
        public Items_Index3()
        {
            Map = items => from i in items
                select new { i.Age, i.Tag };
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void PageMustBeFilledFromTheRemainingMatches(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            for (int i = 0; i < 30; i++)
                session.Store(new Item4 { Age = i }, $"items/{i}");
            session.SaveChanges();
        }

        new Items_Index4().Execute(store);
        Indexes.WaitForIndexing(store);

        store.Maintenance.Send(new StopIndexingOperation());
        using (var session = store.OpenSession())
        {
            session.Delete("items/11");
            session.Delete("items/12");
            session.SaveChanges();
        }

        using var querySession = store.OpenSession();
        var page = querySession.Query<Item4, Items_Index4>().Where(x => x.Age > 10).OrderBy(x => x.Age).Take(5).ToList();

        Assert.Equal(new long[] { 13, 14, 15, 16, 17 }, page.Select(x => x.Age));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void PageMustBeFilledAfterEveryKindOfCut(Options options)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
        {
            Name = "Products/ByCatGrpPrice",
            Maps = { "from p in docs.Products select new { p.Cat, p.Grp, p.Price }" }
        }));
        const int count = 70_000; // more than one 64K container of entry ids
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < count; i++)
                bulk.Store(new Product { Cat = i % 1000 == 999 ? "z" : i < count / 2 ? "a" : "b", Grp = i % 2 == 0 ? "x" : "y", Price = i % 3 }, $"products/{i}");
        }
        Indexes.WaitForIndexing(store);

        store.Maintenance.Send(new StopIndexingOperation());
        var deleted = Enumerable.Range(0, 300).Concat(Enumerable.Range(0, 66_000).Where(i => i % 6 == 4)).Distinct().ToList();
        foreach (var chunk in deleted.Chunk(1000))
        {
            using var session = store.OpenSession();
            foreach (int i in chunk)
                session.Delete($"products/{i}");
            session.SaveChanges();
        }

        foreach (var (where, gate, op) in new (string, long?, string)[]
                 {
                     ("Cat < 'c'", null, "FromTreeScan"), // each posting list of the range is cut at the limit
                     ("Cat = 'a' and Grp = 'x'", 1, "EntryScan"), // the entry scan leaves candidates unchecked
                     ("Price = 1 and Grp = 'x'", -1, "CtxAndFromPostingSource") // the AND drops the containers past the first limit survivors
                 })
        {
            string query = $"from index 'Products/ByCatGrpPrice' where {where}";
            var page = RunRql(store, $"{query} limit 10", entryScanGate: gate);
            RavenTestHelper.AssertAll(() => query,
                () => Assert.True(op == "EntryScan"
                    ? Find(page.Plan, "EntryScan")?.Parameters["Taken"] == "True"
                    : Find(page.Plan, "CompiledQuery").Parameters["CSharpSourceFormatted"].Contains(op), op),
                () => Assert.Equal(RunRql(store, query, entryScanGate: gate).Ids.Take(10), page.Ids));
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void PageMustBeFilledWhenTheLimitCutsAnOperand(Options options)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
        {
            Name = "Item10s/All",
            Maps = { "from i in docs.Item10s select new { i.Tag, i.Age, i.Name, i.Cat, i.G }" }
        }));
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 20_000; i++)
                bulk.Store(new Item10
                {
                    Tag = i >= 1000 ? "t" : "u", Age = i < 100 || i >= 2000 ? 7 : 1, Name = $"n{i}",
                    Cat = i < 200 ? "a" : i is >= 19_990 and < 19_993 ? "c" : "z", G = i < 200 ? "g1" : "g2"
                }, $"items/{i:D5}");
        }
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            session.Advanced.RawQuery<Item10>("from index 'Item10s/All' where Cat = 'a' and Age >= 0 limit 10").Statistics(out var stats).ToList();
            Assert.Equal(200, stats.TotalResults);
        }

        store.Maintenance.Send(new StopIndexingOperation());
        using (var session = store.OpenSession())
        {
            for (int i = 0; i < 40; i++)
                session.Delete($"items/{i:D5}");
            session.SaveChanges();
        }

        foreach (string where in new[] { "Tag = 't' and Age = 7", "Name = 'none' or (Age = 7 and Tag = 't')", "Cat = 'a' or Cat = 'c'", "Cat in ('c', 'a')", "Cat = 'c' or G < 'g2'", "Cat <= 'c'" })
        {
            string query = $"from index 'Item10s/All' where {where}";
            var page = RunRql(store, $"{query} limit 10").Ids;
            RavenTestHelper.AssertAll(() => query,
                () => Assert.Equal(10, page.Length),
                () => Assert.Equal(RunRql(store, query).Ids.Take(10), page));
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void AllInPageMustNotBeCutBeforeTheLastTerm(Options options)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition { Name = "Item10s/ByT", Maps = { "from i in docs.Item10s select new { i.T }" } }));
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 70_000; i++)
            {
                // 'b' and 'c' split the ids below 65,000 at random, so both are large posting lists; only the ids from 66,000 hold all three terms
                bool all = i is >= 66_000 and < 67_000;
                bool half = ((uint)(i * 2654435761L) >> 7) % 2 == 0;
                var t = new List<string>();
                if (all || i < 10)
                    t.Add("a");
                if (all || i < 5 || i is >= 10 and < 65_000 && half)
                    t.Add("b");
                if (all || i is >= 5 and < 10 || i is >= 10 and < 65_000 && half == false)
                    t.Add("c");
                bulk.Store(new Item10 { T = t }, $"items/{i:D5}");
            }
        }
        Indexes.WaitForIndexing(store);

        Assert.Equal(Enumerable.Range(66_000, 5).Select(i => $"items/{i:D5}"), RunRql(store, "from index 'Item10s/ByT' where T all in ('a', 'b', 'c') limit 5").Ids);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void PageMustBeFilledWhenTheLimitedAndCountsAnIdTwice(Options options)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition { Name = "Item10s/ByTTagName", Maps = { "from i in docs.Item10s select new { i.T, i.Tag, i.Name }" } }));
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 70_000; i++)
            {
                var t = i < 10 && i % 2 == 0 ? new List<string> { "a", "b" } : i is >= 66_000 and < 66_100 ? new List<string> { "a" } : new List<string> { "z" };
                bulk.Store(new Item10 { T = t, Tag = "x", Name = $"n{i}" }, $"items/{i:D5}");
            }
        }
        Indexes.WaitForIndexing(store);

        // T in ('a', 'b') puts items 0, 2, 4, 6 and 8 into the slot twice, and the limited AND that follows counts both copies
        const string query = "from index 'Item10s/ByTTagName' where (T in ('a', 'b') and Tag = 'x') or Name = 'none' limit 10";
        Assert.Contains("CtxAndFromPostingSource", Find(RunRql(store, query).Plan, "CompiledQuery").Parameters["CSharpSourceFormatted"]);

        // no include timings(): timings count the slot after every op, which drops the second copies
        using var session = store.OpenSession();
        var page = session.Advanced.RawQuery<Item10>(query).NoCaching().ToList().Select(x => x.Id);
        Assert.Equal(new[] { 0, 2, 4, 6, 8, 66_000, 66_001, 66_002, 66_003, 66_004 }.Select(i => $"items/{i:D5}"), page);
    }

    private class Item10
    {
        public string Id { get; set; }
        public string Tag { get; set; }
        public long Age { get; set; }
        public string Name { get; set; }
        public string Cat { get; set; }
        public string G { get; set; }
        public List<string> T { get; set; }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task HugePageSizeMustKeepTheOffset(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 3000; i++)
                bulk.Store(new Item4 { Age = i }, $"items/{i}");
        }

        new Items_Index4().Execute(store);
        Indexes.WaitForIndexing(store);

        // only a raw query can ask for a page size above int.MaxValue
        using var client = new HttpClient().WithConventions(store.Conventions);
        foreach (string strategy in new[] { null, "FieldSortedScan", "BitmapPipeline" })
        {
            string parameters = strategy is null ? "{}" : $"{{\"rvn_corax_strategy\":\"{strategy}\"}}";
            var response = await client.PostAsync($"{store.Urls[0]}/databases/{store.Database}/queries", new StringContent(
                $"{{\"Query\":\"from index 'Items/Index4' where Age >= 2 order by Age as long\",\"Start\":2500,\"PageSize\":{long.MaxValue},\"SkipStatistics\":true,\"QueryParameters\":{parameters}}}"));
            using var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
            var ids = json.RootElement.GetProperty("Results").EnumerateArray().Select(x => x.GetProperty("@metadata").GetProperty("@id").GetString());

            Assert.Equal(Enumerable.Range(2502, 498).Select(i => $"items/{i}"), ids);
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Highlighting)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void RefilledPageMustKeepTheHighlightings(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            for (int i = 1; i <= 10; i++)
                session.Store(new Item6 { Category = "x", Name = $"chai {i}" }, $"products/{i}");
            session.SaveChanges();
        }

        var highlightingOptions = new HighlightingOptions { PreTags = ["<b>"], PostTags = ["</b>"] };
        using (var session = store.OpenSession())
            Query(session, sorted: false, highlightingOptions, out _).ToList();

        Indexes.WaitForIndexing(store);
        store.Maintenance.Send(new StopIndexingOperation());
        using (var session = store.OpenSession())
        {
            session.Delete("products/2");
            session.SaveChanges();
        }

        foreach (var sorted in new[] { false, true })
        {
            using var session = store.OpenSession();
            var all = Query(session, sorted, highlightingOptions, out var allHighlightings).ToList();
            var page = Query(session, sorted, highlightingOptions, out var pageHighlightings).Take(5).ToList();

            Assert.Equal(9, all.Count);
            Assert.Equal(all.Take(5).Select(x => x.Id), page.Select(x => x.Id));
            Assert.All(page, x => Assert.Equal(allHighlightings.GetFragments(x.Id), pageHighlightings.GetFragments(x.Id)));
        }

        static IQueryable<Item6> Query(IDocumentSession session, bool sorted, HighlightingOptions options, out Highlightings highlightings)
        {
            var query = session.Query<Item6>().Highlight(x => x.Name, 18, 1, options, out highlightings).Where(x => x.Category == "x");
            return sorted ? query.OrderBy(x => x.Name) : query;
        }
    }

    private class Item6
    {
        public string Id { get; set; }
        public string Category { get; set; }
        public string Name { get; set; }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void UnsortedPageMustBeFilledFromTheRemainingMatches(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            for (int i = 0; i < 30; i++)
                session.Store(new Item4 { Age = i }, $"items/{i}");
            session.SaveChanges();
        }

        new Items_Index4().Execute(store);
        Indexes.WaitForIndexing(store);

        store.Maintenance.Send(new StopIndexingOperation());
        using (var session = store.OpenSession())
        {
            session.Delete("items/0");
            session.Delete("items/1");
            session.SaveChanges();
        }

        using var querySession = store.OpenSession();
        var all = querySession.Query<Item4, Items_Index4>().Where(x => x.Age < 10 || x.Age > 25).ToList();
        var page = querySession.Query<Item4, Items_Index4>().Where(x => x.Age < 10 || x.Age > 25).Take(5).ToList();

        Assert.Equal(12, all.Count);
        Assert.Equal(all.Take(5).Select(x => x.Id), page.Select(x => x.Id));

        all = querySession.Query<Item4, Items_Index4>().ToList();
        page = querySession.Query<Item4, Items_Index4>().Take(5).ToList();

        Assert.Equal(28, all.Count);
        Assert.Equal(all.Take(5).Select(x => x.Id), page.Select(x => x.Id));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void RandomPageMustBeFilledFromTheRemainingMatches(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 1000; i++)
                bulk.Store(new Item4 { Age = i }, $"items/{i}");
        }

        new Items_Index4().Execute(store);
        Indexes.WaitForIndexing(store);

        store.Maintenance.Send(new StopIndexingOperation());
        using (var session = store.OpenSession())
        {
            for (int i = 0; i < 1000; i++)
            {
                if (i % 10 != 0)
                    session.Delete($"items/{i}");
            }
            session.SaveChanges();
        }

        using var querySession = store.OpenSession();
        foreach (var seed in new[] { "a", "b", "c", "d", "e" })
        {
            var all = querySession.Query<Item4, Items_Index4>().Customize(x => x.RandomOrdering(seed)).ToList();
            var page = querySession.Query<Item4, Items_Index4>().Customize(x => x.RandomOrdering(seed)).Take(50).ToList();

            Assert.Equal(100, all.Count);
            Assert.Equal(all.Take(50).Select(x => x.Id), page.Select(x => x.Id));
        }

        for (int run = 0; run < 5; run++)
        {
            var page = querySession.Query<Item4, Items_Index4>().Customize(x => x.RandomOrdering()).Take(50).ToList();
            Assert.Equal(50, page.Select(x => x.Id).Distinct().Count());
        }
    }

    private class Item4
    {
        public string Id { get; set; }
        public long Age { get; set; }
    }

    private class Items_Index4 : AbstractIndexCreationTask<Item4>
    {
        public Items_Index4()
        {
            Map = items => from i in items
                select new { i.Age };
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void FilterMustRunBeforeTheGroupIsCut(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 3000; i++)
                bulk.Store(new Item5 { Age = i % 2, Tag = i < 6 ? "t0" : "t1", Name = $"n{i:D4}" });
        }

        new Items_Index5().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();
        var page = session.Query<Item5, Items_Index5>()
            .Where(x => x.Age >= 0 && x.Tag == "t1")
            .OrderBy(x => x.Age)
            .ThenBy(x => x.Name)
            .Take(3)
            .ToList();

        Assert.Equal(new[] { "n0006", "n0008", "n0010" }, page.Select(x => x.Name));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void RefilledPageMustNotRepeatRowsWithEqualSortKeys(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 3000; i++)
                bulk.Store(new Item3 { Tag = "x", Age = i % 3 });
        }

        new Items_Index3().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var first = session.Query<Item3, Items_Index3>().OrderBy(x => x.Tag).ThenByDescending(x => x.Age).Take(20).ToList();

            store.Maintenance.Send(new StopIndexingOperation());
            foreach (var item in first.Take(5))
                session.Delete(item);
            session.SaveChanges();
        }

        using var querySession = store.OpenSession();
        var page = querySession.Query<Item3, Items_Index3>().OrderBy(x => x.Tag).ThenByDescending(x => x.Age).Take(20).ToList();

        Assert.Equal(20, page.Select(x => x.Id).Distinct().Count());
        Assert.All(page, x => Assert.Equal(2, x.Age));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void GroupCutAmongEntriesThatPassTheFilterMustMatchTheBitmap(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            // one rare value of Q per 97 documents, so a group cut has to check far more entries than it keeps
            for (int i = 0; i < 6000; i++)
                bulk.Store(new Item7 { G = i % 2, S = i % 13 == 0 ? null : i * 7 % 50, Q = i % 97 == 0 ? "r" : "n" });
        }

        new Items_Index7().Execute(store);
        Indexes.WaitForIndexing(store);

        const string rql = "from index 'Items/Index7' where G >= 0 and Q = 'r' order by G as long, S as long desc limit 256";
        var bitmap = RunRql(store, rql, "BitmapPipeline");
        var walk = RunRql(store, rql, "FieldSortedScan");

        Assert.Equal("FieldSortedScan", walk.Hint);
        Assert.Equal(62, bitmap.Ids.Length);
        Assert.Equal(bitmap.Ids, walk.Ids);
    }

    private class Item7
    {
        public string Id { get; set; }
        public long G { get; set; }
        public long? S { get; set; }
        public string Q { get; set; }
    }

    private class Items_Index7 : AbstractIndexCreationTask<Item7>
    {
        public Items_Index7()
        {
            Map = items => from i in items
                select new { i.G, i.S, i.Q };
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TwoKeyWalkMustMatchTheBitmapWhenARefillReadsTruncatedGroupsAgain(Options options)
    {
        using var store = GetDocumentStore(options);
        const string map = "from i in docs.Item9s select new { i.Cat, i.Flag, i.Num, i.Name }";
        store.Maintenance.Send(new PutIndexesOperation(
            new IndexDefinition { Name = "Item9s/NullsSmallest", Maps = { map } },
            new IndexDefinition { Name = "Item9s/NullsLargest", Maps = { map }, Configuration = new IndexConfiguration { ["Indexing.Querying.Corax.NullsSortMode"] = "NullsLargest" } }));
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 3000; i++)
                bulk.Store(new Item9 { Cat = "a", Flag = i % 3 == 0 ? "x" : "y", Num = i % 11 == 0 ? null : i * 7919 % 3000 / 7.0, Name = i > 2000 && i % 50 == 0 ? null : $"n{i * 7919 % 3000:D4}" }, $"items/{i}");
        }
        Indexes.WaitForIndexing(store);

        store.Maintenance.Send(new StopIndexingOperation());
        using (var session = store.OpenSession())
        {
            for (int i = 0; i < 3000; i += 5)
                session.Delete($"items/{i}");
            session.SaveChanges();
        }

        foreach (string index in new[] { "Item9s/NullsSmallest", "Item9s/NullsLargest" })
        foreach (string order in new[] { "Num as double", "Num as double desc", "Name", "Name desc" })
        foreach (string page in new[] { "limit 10", "limit 10 offset 5" })
        {
            string query = $"from index '{index}' where Cat >= 'a' and Flag = 'x' order by Cat, {order} {page}";
            var walk = RunRql(store, query, "FieldSortedScan");
            RavenTestHelper.AssertAll(() => query,
                () => Assert.Equal("FieldSortedScan", walk.Hint),
                () => Assert.Equal(RunRql(store, query, "BitmapPipeline").Ids, walk.Ids));
        }
    }

    private class Item9
    {
        public string Id { get; set; }
        public string Cat { get; set; }
        public string Flag { get; set; }
        public double? Num { get; set; }
        public string Name { get; set; }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TwoKeyWalkDoesNotReadAGroupThePageDoesNotNeed(Options options)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
        {
            Name = "Products/ByCatPrice",
            Maps = { "from p in docs.Products select new { p.Cat, p.Price, p.Grp }" }
        }));
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 61 * 17; i++)
                bulk.Store(new Product { Cat = $"a{i / 17:D2}", Price = i, Grp = i / 17 > 50 && i % 17 == 0 ? "rare" : "common" }, $"products/{i}");
            for (int i = 61 * 17; i < 61 * 17 + 20_000; i++)
                bulk.Store(new Product { Cat = "b", Price = i, Grp = i % 100 == 0 ? "rare" : "common" }, $"products/{i}");
        }
        Indexes.WaitForIndexing(store);

        // the last row of the page is the last entry of Cat 'a60', so the walk has no reason to read the 20,000 entries of Cat 'b'
        const string query = "from index 'Products/ByCatPrice' where Cat >= 'a' and Grp = 'rare' order by Cat, Price as long desc limit 10";
        var walk = RunRql(store, query, "FieldSortedScan");
        RavenTestHelper.AssertAll(() => query,
            () => Assert.Equal("FieldSortedScan", walk.Hint),
            () => Assert.Equal(Enumerable.Range(51, 10).Select(g => $"products/{g * 17}"), walk.Ids),
            () => Assert.Equal(walk.Ids, RunRql(store, query, "BitmapPipeline").Ids),
            () => Assert.InRange(long.Parse(Find(walk.Plan, "DirectScan").Parameters["GroupEntriesRead"], System.Globalization.NumberStyles.AllowThousands,
                System.Globalization.CultureInfo.InvariantCulture), 0, 61 * 17),
            () => Assert.Equal(walk.Ids.Skip(3), RunRql(store, query.Replace("limit 10", "limit 7 offset 3"), "FieldSortedScan").Ids));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TwoKeyWalkTruncatesAGroupAmongTheEntriesThatPassTheResidual(Options options)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
        {
            Name = "Products/ByCatPrice",
            Maps = { "from p in docs.Products select new { p.Cat, p.Price, p.Grp }" }
        }));
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 3000; i++)
                bulk.Store(new Product { Cat = "a", Price = i, Grp = $"g{i * 7 % 3000:D4}" }, $"products/{i}");
        }
        Indexes.WaitForIndexing(store);

        foreach (var (filter, first, step) in new[]
                 {
                     ("Price >= 100 order by Cat, Price as long", 100, 1),
                     ("Price between 1100 and 1200 order by Cat, Price as long", 1100, 1),
                     ("Price < 1000 order by Cat, Price as long desc", 999, -1),
                     ("Price >= 2900 order by Cat, Price as long", 2900, 1),
                     ("Price < 2900 order by Cat, Price as long desc", 2899, -1),
                     ("Price < 700 order by Cat, Price as long desc", 699, -1),
                     ("Price < 778 order by Cat, Price as long desc", 777, -1)
                 })
        {
            string query = $"from index 'Products/ByCatPrice' where Cat >= 'a' and {filter} limit 10";
            var expected = Enumerable.Range(0, 10).Select(k => $"products/{first + k * step}").ToArray();
            var scan = RunRql(store, query, "FieldSortedScan");
            RavenTestHelper.AssertAll(() => query,
                () => Assert.Equal("FieldSortedScan", scan.Hint),
                () => Assert.Equal(expected, scan.Ids),
                () => Assert.Equal(expected, RunRql(store, query).Ids));
        }

        // Grp does not follow the id, so a later round brings entries that rank before the kept ones as well as after them
        foreach (string order in new[] { "Grp", "Grp desc" })
        {
            string query = $"from index 'Products/ByCatPrice' where Cat >= 'a' and Price >= 100 order by Cat, {order} limit 10";
            var scan = RunRql(store, query, "FieldSortedScan");
            RavenTestHelper.AssertAll(() => query,
                () => Assert.Equal("FieldSortedScan", scan.Hint),
                () => Assert.Equal(RunRql(store, query, "BitmapPipeline").Ids, scan.Ids));
        }

        const string gate = "from index 'Products/ByCatPrice' where Cat >= 'a' and Price < 1500 order by Cat, Price as long desc limit 10";
        var walk = RunRql(store, gate);
        RavenTestHelper.AssertAll(() => gate,
            () => Assert.Equal("FieldSortedScan", walk.Hint),
            () => Assert.Equal(RunRql(store, gate, "BitmapPipeline").Ids, walk.Ids));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TwoKeyCostGatePricesTheGroupsTheWalkReaches(Options options)
    {
        var index = new IndexDefinition { Name = "Products/ByCatPrice", Maps = { "from p in docs.Products select new { p.Cat, p.Rank, p.Price, p.Grp, p.Tier }" } };

        // the walk starts in the group of Cat 'a' (Rank 1000 when walked down), which holds 3000 of the 3300 entries, and truncates it in rounds;
        // the 300 groups after it make the average group small
        using (var store = GetDocumentStore(options))
        {
            store.Maintenance.Send(new PutIndexesOperation(index));
            using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < 3000; i++)
                    bulk.Store(new Product { Cat = "a", Rank = 1000, Price = i }, $"products/{i}");
                for (int i = 0; i < 300; i++)
                    bulk.Store(new Product { Cat = $"b{i:D3}", Rank = i, Price = 3000 + i }, $"products/{3000 + i}");
            }
            Indexes.WaitForIndexing(store);

            foreach (string query in new[]
                     {
                         "from index 'Products/ByCatPrice' where Cat >= 'a' and Price < 360 order by Cat, Price as long desc limit 10",
                         "from index 'Products/ByCatPrice' where Rank >= 0 and Price < 360 order by Rank as long desc, Price as long desc limit 10"
                     })
            {
                var page = RunRql(store, query);
                RavenTestHelper.AssertAll(() => query,
                    () => Assert.Equal("BitmapPipeline", page.Hint),
                    () => Assert.Equal(Enumerable.Range(350, 10).Reverse().Select(i => $"products/{i}"), page.Ids),
                    () => Assert.Equal(page.Ids, RunRql(store, query, "FieldSortedScan").Ids));
            }
        }

        // ten small groups of Cat 'a00'..'a09' come before the group of Cat 'z', which makes the average group large; Price does not follow the id
        using (var store = GetDocumentStore(options))
        {
            store.Maintenance.Send(new PutIndexesOperation(index));
            using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < 16_500; i++)
                    bulk.Store(new Product { Cat = i < 500 ? $"a{i / 50:D2}" : "z", Price = i * 7919L % 16_500, Grp = i % 10 == 0 ? "x" : "y", Tier = i % 5 == 0 ? "x" : "y" }, $"products/{i}");
            }
            Indexes.WaitForIndexing(store);

            // the small groups fill the page before the walk reaches Cat 'z', or they leave two rows to it, and every round of 'z' checks the whole take again
            foreach (var (query, hint) in new[]
                     {
                         ("from index 'Products/ByCatPrice' where Cat >= 'a' and Grp = 'x' order by Cat, Price as long limit 50", "FieldSortedScan"),
                         ("from index 'Products/ByCatPrice' where Cat >= 'a' and Tier = 'x' order by Cat, Price as long limit 102", "BitmapPipeline")
                     })
            {
                var page = RunRql(store, query);
                RavenTestHelper.AssertAll(() => query,
                    () => Assert.Equal(hint, page.Hint),
                    () => Assert.Equal(RunRql(store, query, hint == "BitmapPipeline" ? "FieldSortedScan" : "BitmapPipeline").Ids, page.Ids));
            }
        }
    }

    private class Product
    {
        public string Id { get; set; }
        public string Cat { get; set; }
        public long Rank { get; set; }
        public long Price { get; set; }
        public string Grp { get; set; }
        public string Tier { get; set; }
    }

    private static (string[] Ids, string Hint, QueryInspectionNode Plan) RunRql(IDocumentStore store, string rql, string strategy = null, long? entryScanGate = null)
    {
        using var session = store.OpenSession();
        var query = session.Advanced.RawQuery<Product>($"{rql} include timings()")
            .NoCaching()
            .Timings(out QueryTimings timings);
        if (strategy != null)
            query = query.AddParameter("rvn_corax_strategy", strategy);
        if (strategy == "BitmapPipeline")
            query = query.AddParameter("rvn_corax_entry_scan", -1L); // the reference checks no clause per document
        if (entryScanGate != null)
            query = query.AddParameter("rvn_corax_entry_scan", entryScanGate.Value);

        var ids = query.ToList().Select(x => x.Id).ToArray();
        var plan = (QueryInspectionNode)timings.QueryPlan;
        string hint = null;
        Find(plan, "CompiledQuery")?.Parameters?.TryGetValue("OptimizationHint", out hint);
        return (ids, hint, plan);
    }

    private static QueryInspectionNode Find(QueryInspectionNode node, string operation)
    {
        if (node == null || node.Operation == operation)
            return node;
        return node.Children?.Select(c => Find(c, operation)).FirstOrDefault(x => x != null);
    }

    private class Item5
    {
        public string Id { get; set; }
        public long Age { get; set; }
        public string Tag { get; set; }
        public string Name { get; set; }
    }

    private class Items_Index5 : AbstractIndexCreationTask<Item5>
    {
        public Items_Index5()
        {
            Map = items => from i in items
                select new { i.Age, i.Tag, i.Name };
        }
    }
}
