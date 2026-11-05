using System;
using System.Linq;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

[Collection(SortHeapAllocationsCollection.Name)]
public class SortHeapAllocations(ITestOutputHelper output) : RavenTestBase(output)
{
    private const int Documents = 26_000;
    private const int Take = 20;

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void SortingByScoreWithAHeapDoesNotAllocateForEveryMatch()
    {
        const string rql = "from 'Items' where search(Text, 'alpha') order by score() limit 0, 20";

        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        var random = new Random(3);
        using (var bulk = store.BulkInsert())
        {
            for (var i = 0; i < Documents; i++)
            {
                var words = Enumerable.Range(0, 1 + random.Next(40)).Select(w => w == 0 || random.Next(3) == 0 ? "alpha" : "filler");
                bulk.Store(new Item { Text = string.Join(' ', words) });
            }
        }

        // the first query creates the auto index
        using (var session = store.OpenSession())
            session.Advanced.RawQuery<Item>(rql).ToList();
        Indexes.WaitForIndexing(store);

        Run(store, rql);
        Run(store, rql);

        // the minimum of several runs excludes noise from the process
        var allocated = Enumerable.Range(0, 7).Select(_ => Run(store, rql)).Min();
        output.WriteLine($"allocated by a query: {allocated / 1024.0:N0} KB for {Documents:N0} matches = {allocated / (double)Documents:F1} bytes per match");

        // a boxed term per match would exceed this bound
        Assert.True(allocated < Documents * 16L, $"The query allocated {allocated:N0} bytes for {Documents:N0} matches");
    }

    private static long Run(IDocumentStore store, string rql)
    {
        using var session = store.OpenSession();
        var before = GC.GetTotalAllocatedBytes(precise: true);
        var results = session.Advanced.RawQuery<Item>(rql).NoCaching().ToList();
        var after = GC.GetTotalAllocatedBytes(precise: true);
        Assert.Equal(Take, results.Count);
        return after - before;
    }

    private class Item
    {
        public string Text { get; set; }
    }
}

// counts allocations of the whole process, so no other test may run in parallel
[CollectionDefinition(Name, DisableParallelization = true)]
public class SortHeapAllocationsCollection
{
    public const string Name = nameof(SortHeapAllocationsCollection);
}
