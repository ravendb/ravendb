using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Voron.Data.RawData;
using Xunit;

namespace FastTests.Voron;

public class BulkDocsPrefetchTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private class Item
    {
        public string Name { get; set; }
        public int Version { get; set; }
    }

    private class Person
    {
        public string Name { get; set; }
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(100)]
    [InlineData(64 * 1024)] // a large value: its own overflow pages
    public async Task TheCollectionOfADocumentComesFromItsPageHeader(int size)
    {
        using var store = GetDocumentStore(new Options { RunInMemory = false });
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Item { Name = new string('x', size) }, "items/1");
            await session.SaveChangesAsync();
        }
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Person { Name = new string('y', size) }, "people/1"); // a collection added by a later transaction
            await session.SaveChangesAsync();
        }

        var database = await GetDatabase(store.Database);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var llt = context.Transaction.InnerTransaction.LowLevelTransaction;
            Assert.True(llt.TryGetClientState(out DocumentTransactionCache cache));
            foreach (var (id, collection) in new[] { ("items/1", "Items"), ("people/1", "People") })
            {
                using (var document = database.DocumentsStorage.Get(context, id))
                    Assert.Equal(collection, cache.CollectionsByDocumentsTableOwnerHash[RawDataSection.GetOwnerHash(llt, document.StorageId)].Name);
            }
        }
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

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.ClientApi)]
    public void WhitespaceIdsKeepTheirMeaning()
    {
        using var store = GetDocumentStore(new Options { RunInMemory = false });
        using (var session = store.OpenSession())
        {
            // a put stores the document under an id the server generates; a delete deletes nothing
            var metadata = new DynamicJsonValue { [Raven.Client.Constants.Documents.Metadata.Collection] = "Items" };
            session.Advanced.Defer(new PutCommandData(" ", null, new DynamicJsonValue { ["Name"] = "x", [Raven.Client.Constants.Documents.Metadata.Key] = metadata }));
            session.Advanced.Defer(new DeleteCommandData(" ", null));
            session.SaveChanges();
        }

        Assert.Equal(1, store.Maintenance.Send(new GetCollectionStatisticsOperation()).Collections["Items"]);
    }
}
