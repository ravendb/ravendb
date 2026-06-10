using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26792(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public async Task FreeSpaceSnapshotEndpointShouldReturnTheRequestedEnvironment()
    {
        using (var store = GetDocumentStore())
        {
            await new Orders_ByName().ExecuteAsync(store);
            await Indexes.WaitForIndexingAsync(store);

            var random = new Random(2024);
            const string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            var chars = new char[64 * 1024];
            for (var i = 0; i < chars.Length; i++)
                chars[i] = alphabet[random.Next(alphabet.Length)];
            var payload = new string(chars);

            using (var session = store.OpenSession())
            {
                for (var i = 0; i < 8; i++)
                    session.Store(new Item { Payload = payload }, "items/" + i);
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                for (var i = 0; i < 8; i++)
                    session.Delete("items/" + i);
                session.SaveChanges();
            }

            var database = await GetDatabase(store.Database);
            var documentsEnvironment = database.DocumentsStorage.Environment;
            var index = database.IndexStore.GetIndexes().Single();
            var indexEnvironment = index._indexStorage.Environment();

            long documentsFreePages;
            long indexFreePages;
            using (var dbReadTransaction = documentsEnvironment.ReadTransaction())
                documentsFreePages = documentsEnvironment.FreeSpaceHandling.GetFreePagesCount(dbReadTransaction.LowLevelTransaction);
            using (var indexReadTransaction = indexEnvironment.ReadTransaction())
                indexFreePages = indexEnvironment.FreeSpaceHandling.GetFreePagesCount(indexReadTransaction.LowLevelTransaction);

            Assert.True(documentsFreePages > indexFreePages + 16,
                $"fixture failed to diverge the environments: documents={documentsFreePages}, index={indexFreePages}");

            using (var commands = store.Commands())
            {
                var documentsJson = commands.RawGetJson<BlittableJsonReaderObject>(
                    $"/debug/storage/environment/free-space-snapshot?name={Uri.EscapeDataString(store.Database)}&type=Documents");
                Assert.True(documentsJson.TryGet("FreePagesCount", out long endpointDocumentsFreePages));

                var indexJson = commands.RawGetJson<BlittableJsonReaderObject>(
                    $"/debug/storage/environment/free-space-snapshot?name={Uri.EscapeDataString(index.Name)}&type=Index");
                Assert.True(indexJson.TryGet("FreePagesCount", out long endpointIndexFreePages));

                Assert.Equal(documentsFreePages, endpointDocumentsFreePages);
                Assert.Equal(indexFreePages, endpointIndexFreePages);
            }
        }
    }

    private class Item
    {
        public string Payload { get; set; }
    }

    private class Order
    {
        public string Name { get; set; }
    }

    private class Orders_ByName : AbstractIndexCreationTask<Order>
    {
        public Orders_ByName()
        {
            Map = orders => from order in orders
                select new
                {
                    order.Name
                };
        }
    }
}
