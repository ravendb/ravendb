using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Voron;

public class BulkDocsPrefetchTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private class Item
    {
        public string Name { get; set; }
        public int Version { get; set; }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public async Task ExistingDocumentsHaveTheirPagesReadBeforeTheMerger()
    {
        using var store = GetDocumentStore(new Options { RunInMemory = false }); // an in-memory data file has no prefetch ring
        var ids = Enumerable.Range(0, 50).Select(i => $"items/{i}").ToArray();
        using (var session = store.OpenAsyncSession())
        {
            foreach (var id in ids)
                await session.StoreAsync(new Item { Name = id }, id);
            await session.SaveChangesAsync();
        }

        var database = await GetDatabase(store.Database);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var read = database.DocumentsStorage.PrefetchDocumentsAsync(context, ids.Append("items/missing").ToList());
            Assert.True(read.AsTask().Wait(TimeSpan.FromSeconds(10)));
        }

        // a batch that puts, patches and deletes existing documents (and adds a new one) goes through the prefetch
        using (var session = store.OpenAsyncSession())
        {
            var items = await session.LoadAsync<Item>(ids.Take(40));
            foreach (var item in items.Values)
                item.Version++;
            session.Advanced.Patch<Item, int>(ids[40], x => x.Version, 7);
            session.Delete(ids[41]);
            await session.StoreAsync(new Item { Name = "new" }, "items/");
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            var items = await session.LoadAsync<Item>(ids);
            Assert.All(ids.Take(40), id => Assert.Equal(1, items[id].Version));
            Assert.Equal(7, items[ids[40]].Version);
            Assert.Null(items[ids[41]]);
            Assert.Equal(0, items[ids[42]].Version);
        }
    }
}
