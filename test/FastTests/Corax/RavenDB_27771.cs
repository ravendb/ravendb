using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

public class RavenDB_27771(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    public async Task PendingContainerFreesAreReclaimedByBatchAfterWrites()
    {
        const int count = 5_000;
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        await new Items_ByName().ExecuteAsync(store);

        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < count; i++)
                await bulk.StoreAsync(new Item { Name = $"name-{i:D6}" }, $"items/{i}");
        }

        await Indexes.WaitForIndexingAsync(store);
        await store.Maintenance.SendAsync(new StopIndexingOperation());

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < count; i += 100)
                session.Delete($"items/{i}");
            await session.SaveChangesAsync();
        }

        var database = await GetDocumentDatabaseInstanceFor(store);
        var index = database.IndexStore.GetIndex(new Items_ByName().IndexName);

        long pendingAfterLastBatchWithWork = 0;
        while (index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None))
            pendingAfterLastBatchWithWork = PendingFrees(index);

        Assert.True(pendingAfterLastBatchWithWork > 0);
        Assert.Equal(0, PendingFrees(index));
    }

    private static long PendingFrees(Index index) =>
        index.GenerateStorageReport(calculateExactSizes: false).Trees.Where(x => x.Name.EndsWith("(Container Pending Frees)")).Sum(x => x.NumberOfEntries);

    private class Item
    {
        public string Name { get; set; }
    }

    private class Items_ByName : AbstractIndexCreationTask<Item>
    {
        public Items_ByName()
        {
            Map = items => from item in items
                select new { item.Name };
        }
    }
}
