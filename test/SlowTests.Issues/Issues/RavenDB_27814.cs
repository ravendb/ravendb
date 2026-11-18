using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27814 : RavenTestBase
{
    public RavenDB_27814(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData("where Age > 10 order by Age as long", SearchEngineMode = RavenSearchEngineMode.Corax)]
    [RavenData("where Age > 10 order by Age as long desc", SearchEngineMode = RavenSearchEngineMode.Corax)]
    [RavenData("where Age > 10 order by Age as long, Name", SearchEngineMode = RavenSearchEngineMode.Corax)]
    [RavenData("where Name > 'n050' order by Name", SearchEngineMode = RavenSearchEngineMode.Corax)]
    // an unscored boost() is planned as if it were not boosted, so these reach the walk unforced too
    [RavenData("where boost(Age > 10, 2) order by Age as long limit 5", SearchEngineMode = RavenSearchEngineMode.Corax)]
    [RavenData("where Age >= 0 and boost(Name != 'n014', 2) order by Age as long limit 5", SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void FieldSortedScanSkipsDocumentsWithoutTheSortValue(Options options, string query)
    {
        using var store = GetDocumentStore(options);
        Seed(store);

        string rql = $"from index 'Items/ByAll' {query}";
        var bitmap = RunRql(store, rql, "BitmapPipeline");
        var scan = RunRql(store, rql, "FieldSortedScan");
        Assert.Equal("FieldSortedScan", scan.Hint);
        Assert.Equal(bitmap.Ids, scan.Ids);
        Assert.Equal(bitmap.Ids, RunRql(store, rql).Ids);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void OpenBetweenDrivingTheScanSkipsDocumentsWithoutTheField(Options options)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
        {
            Name = "Items/ByName",
            Maps = { "from d in docs.Items select new { d.Name, d.Other }" }
        }));

        using (var commands = store.Commands())
        {
            var meta = new Dictionary<string, object> { ["@collection"] = "Items" };
            for (int i = 0; i < 100; i++)
                commands.Put($"items/{i}", null, new Dictionary<string, object> { ["Name"] = $"n{i:D3}" }, meta);
            commands.Put("items/unnamed", null, new Dictionary<string, object> { ["Other"] = 1L }, meta);
        }

        Indexes.WaitForIndexing(store);

        var openHigh = RunRql(store, "from index 'Items/ByName' where Name between 'n095' and 'NULL' order by Name", "FieldSortedScan");
        Assert.Equal("FieldSortedScan", openHigh.Hint);
        Assert.Equal(["items/95", "items/96", "items/97", "items/98", "items/99"], openHigh.Ids);

        var openLow = RunRql(store, "from index 'Items/ByName' where Name between '*' and 'n004' order by Name", "FieldSortedScan");
        Assert.Equal("FieldSortedScan", openLow.Hint);
        Assert.Equal(["items/0", "items/1", "items/2", "items/3", "items/4"], openLow.Ids);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void EqualsNullDoesNotDriveTheSortedWalk(Options options)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
        {
            Name = "Items/ByTags",
            Maps = { "from d in docs.Items select new { d.Tags, d.Other }" }
        }));

        using (var commands = store.Commands())
        {
            var meta = new Dictionary<string, object> { ["@collection"] = "Items" };
            for (int i = 0; i < 10; i++)
                commands.Put($"items/{i}", null, new Dictionary<string, object> { ["Tags"] = new[] { "a", "x" + i } }, meta);
            commands.Put("items/null1", null, new Dictionary<string, object> { ["Tags"] = null }, meta);
            commands.Put("items/null2", null, new Dictionary<string, object> { ["Tags"] = null }, meta);
            commands.Put("items/none", null, new Dictionary<string, object> { ["Other"] = 1L }, meta);
        }

        Indexes.WaitForIndexing(store);

        // Tags is multi-valued, so `Tags = null` keeps its sort key and can drive the walk when forced
        const string query = "from index 'Items/ByTags' where Tags = null order by Tags";
        var bitmap = RunRql(store, query, "BitmapPipeline");
        var forced = RunRql(store, query, "FieldSortedScan");
        Assert.Equal("BitmapPipeline", forced.Hint);
        Assert.Equal(["items/null1", "items/null2"], bitmap.Ids.Order());
        Assert.Equal(bitmap.Ids, forced.Ids);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void RangeDrivenScanMustSkipNullAndMissingValues(Options options)
    {
        options.ModifyDocumentStore = s => s.Conventions.FindCollectionName = _ => "Items";
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            for (int i = 0; i < 20; i++)
                session.Store(new Item { Name = $"n{i:D3}" });
            session.Store(new Item { Name = null });
            session.Store(new ItemWithoutName());
            session.SaveChanges();
        }

        new Items_Index().Execute(store);
        Indexes.WaitForIndexing(store);

        using var querySession = store.OpenSession();
        var page = querySession.Query<Item, Items_Index>()
            .Where(x => x.Name.CompareTo("n010") > 0)
            .OrderBy(x => x.Name)
            .Take(3)
            .ToList();

        Assert.Equal(new[] { "n011", "n012", "n013" }, page.Select(x => x.Name));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TwoKeyWalkOrdersEqualKeysLikeTheBitmap(Options options)
    {
        using var store = GetDocumentStore(options);
        SeedTwoKeys(store);

        foreach (var order in new[] { "Cat, N as long", "Cat, N as long desc", "Cat, D as double desc", "Cat, S", "Cat, S desc", "Cat desc, N as long desc" })
            AssertTwoKeyWalkMatchesBitmap(store, "Items/ByCat", order);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TwoKeyWalkPlacesNullStringSecondariesLikeTheBitmap(Options options)
    {
        using var store = GetDocumentStore(options);
        SeedTwoKeys(store);

        foreach (var order in new[] { "Cat, S nulls last", "Cat, S desc nulls first", "Cat desc, S nulls last" })
            AssertTwoKeyWalkMatchesBitmap(store, "Items/ByCat", order);
        foreach (var order in new[] { "Cat, S", "Cat, S desc" })
            AssertTwoKeyWalkMatchesBitmap(store, "Items/ByCatNullsLargest", order);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void SecondaryNullsMustFollowTheNullsClause(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Item2 { Age = 1, Name = "b" });
            session.Store(new Item2 { Age = 1, Name = null });
            session.Store(new Item2 { Age = 1, Name = "a" });
            session.SaveChanges();
        }

        new Items_Index2().Execute(store);
        Indexes.WaitForIndexing(store);

        using var querySession = store.OpenSession();
        var results = querySession.Query<Item2, Items_Index2>()
            .OrderBy(x => x.Age)
            .ThenBy(x => x.Name, NullsOrdering.Last)
            .ToList();

        Assert.Equal(new[] { "a", "b", null }, results.Select(x => x.Name));
    }

    private static void AssertTwoKeyWalkMatchesBitmap(IDocumentStore store, string index, string order)
    {
        foreach (var filter in new[] { "", " and R != 3" })
        foreach (var page in new[] { "", " limit 7 offset 13" })
        {
            string query = $"from index '{index}' where Cat >= 'a'{filter} order by {order}{page}";
            var scan = RunRql(store, query, "FieldSortedScan");
            RavenTestHelper.AssertAll(() => query,
                () => Assert.Equal("FieldSortedScan", scan.Hint),
                () => Assert.Equal(RunRql(store, query, "BitmapPipeline").Ids, scan.Ids));
        }
    }

    private void SeedTwoKeys(IDocumentStore store)
    {
        const string map = "from d in docs.Items select new { d.Cat, d.N, d.D, d.S, d.R }";
        store.Maintenance.Send(new PutIndexesOperation(
            new IndexDefinition { Name = "Items/ByCat", Maps = { map } },
            new IndexDefinition { Name = "Items/ByCatNullsLargest", Maps = { map }, Configuration = new IndexConfiguration { ["Indexing.Querying.Corax.NullsSortMode"] = "NullsLargest" } }));

        using (var commands = store.Commands())
        {
            var meta = new Dictionary<string, object> { ["@collection"] = "Items" };
            for (int i = 0; i < 600; i++)
            {
                var doc = new Dictionary<string, object>
                {
                    ["Cat"] = "abc"[i % 3].ToString(),
                    ["N"] = i % 29 == 0 ? null : (long)(i / 3 % 5),
                    ["D"] = i / 3 % 3 + 0.5,
                    ["S"] = i % 23 == 0 ? null : $"s{i / 3 % 4}",
                    ["R"] = (long)(i % 7)
                };
                if (i % 31 == 0)
                    doc.Remove("N");
                if (i % 19 == 0)
                    doc.Remove("S");
                commands.Put($"items/{i}", null, doc, meta);
            }
        }

        Indexes.WaitForIndexing(store);
    }

    private void Seed(IDocumentStore store)
    {
        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
        {
            Name = "Items/ByAll",
            Maps = { "from d in docs.Items select new { d.Name, d.Tag, d.Age, d.Score, d.Tags }" }
        }));

        using var commands = store.Commands();
        var meta = new Dictionary<string, object> { ["@collection"] = "Items" };
        for (int i = 0; i < 100; i++)
        {
            commands.Put($"items/{i}", null, new Dictionary<string, object>
            {
                ["Name"] = $"n{i:D3}",
                ["Tag"] = (i % 10).ToString(),
                ["Age"] = (long)i,
                ["Score"] = i + 0.5,
                ["Tags"] = new[] { (i % 10).ToString(), "x" + i % 3 }
            }, meta);
        }

        var edges = new (string Field, object Value)[] { ("Age", null), ("Age", "abc"), ("Score", null), ("Score", "abc"), ("Name", null), ("Tags", System.Array.Empty<string>()) };
        var missing = new[] { "Age", "Score", "Name", "Tags" };
        for (int k = 0; k < edges.Length + missing.Length; k++)
        {
            var doc = new Dictionary<string, object> { ["Name"] = $"n9{k}", ["Tag"] = "4", ["Age"] = 200L + k, ["Score"] = 60.5, ["Tags"] = new[] { "4", "x0" } };
            if (k < edges.Length)
                doc[edges[k].Field] = edges[k].Value;
            else
                doc.Remove(missing[k - edges.Length]);
            commands.Put($"items/e{k}", null, doc, meta);
        }

        Indexes.WaitForIndexing(store);
    }

    private static (string[] Ids, string Hint) RunRql(IDocumentStore store, string rql, string strategy = null)
    {
        using var session = store.OpenSession();
        var query = session.Advanced.RawQuery<Item>($"{rql} include timings()")
            .NoCaching()
            .Timings(out QueryTimings timings);
        if (strategy != null)
            query = query.AddParameter("rvn_corax_strategy", strategy);
        if (strategy == "BitmapPipeline")
            query = query.AddParameter("rvn_corax_entry_scan", -1L); // the reference checks no clause per document

        var ids = query.ToList().Select(x => x.Id).ToArray();
        string hint = null;
        Find((QueryInspectionNode)timings.QueryPlan, "CompiledQuery")?.Parameters?.TryGetValue("OptimizationHint", out hint);
        return (ids, hint);
    }

    private static QueryInspectionNode Find(QueryInspectionNode node, string operation)
    {
        if (node == null || node.Operation == operation)
            return node;
        return node.Children?.Select(c => Find(c, operation)).FirstOrDefault(x => x != null);
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class ItemWithoutName
    {
        public string Id { get; set; }
    }

    private class Items_Index : AbstractIndexCreationTask<Item>
    {
        public Items_Index()
        {
            Map = items => from i in items
                select new { i.Name };
        }
    }

    private class Item2
    {
        public string Id { get; set; }
        public long Age { get; set; }
        public string Name { get; set; }
    }

    private class Items_Index2 : AbstractIndexCreationTask<Item2>
    {
        public Items_Index2()
        {
            Map = items => from i in items
                select new { i.Age, i.Name };
        }
    }
}
