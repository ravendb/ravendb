using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27813 : RavenTestBase
{
    public RavenDB_27813(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void SeekMustStartAtTheBoundThatOpensTheRange(Options options)
    {
        using var store = SeedNames(options);

        Assert.Equal(Names(20, 19, 18, 17, 16), StreamedNames(store, "from index 'Items/Index' where Name between '*' and 'n020' order by Name desc limit 5"));
        Assert.Equal(Names(150, 151, 152, 153, 154), StreamedNames(store, "from index 'Items/Index' where Name between 'n150' and 'NULL' order by Name limit 5"));
        Assert.Equal(Names(199, 198, 197, 196, 195), StreamedNames(store, "from index 'Items/Index' where Name between 'n150' and 'NULL' order by Name desc limit 5"));
        Assert.Equal(Names(0, 1, 2, 3, 4), StreamedNames(store, "from index 'Items/Index' where Name between '*' and 'n020' order by Name limit 5"));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void DescendingSeekMustUseTheAnalyzedBound(Options options)
    {
        using var store = SeedNames(options);

        Assert.Equal(Names(149, 148, 147, 146, 145), StreamedNames(store, "from index 'Items/Index' where Name < 'N150' order by Name desc limit 5"));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void SeekFromAnEmptyStringBound(Options options)
    {
        using var store = SeedNames(options, withEmptyName: true);

        Assert.Equal(new[] { "" }.Concat(Names(0, 1, 2, 3)), StreamedNames(store, "from index 'Items/Index' where Name >= '' order by Name limit 5"));
    }

    private static string[] Names(params int[] numbers) => numbers.Select(i => $"n{i:D3}").ToArray();

    private static string[] StreamedNames(IDocumentStore store, string rql)
    {
        using var session = store.OpenSession();
        return session.Advanced.RawQuery<Item>(rql).NoCaching()
            .AddParameter("rvn_corax_strategy", "BitmapPipeline")
            .AddParameter("rvn_corax_sort", "IndexOrderStreaming")
            .ToList().Select(x => x.Name).ToArray();
    }

    private IDocumentStore SeedNames(Options options, bool withEmptyName = false)
    {
        var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 200; i++)
                bulk.Store(new Item { Name = $"n{i:D3}" });
            if (withEmptyName)
                bulk.Store(new Item { Name = "" });
        }

        new Items_Index().Execute(store);
        Indexes.WaitForIndexing(store);
        return store;
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class Items_Index : AbstractIndexCreationTask<Item>
    {
        public Items_Index()
        {
            Map = items => from i in items
                select new { i.Name };
        }
    }
}
