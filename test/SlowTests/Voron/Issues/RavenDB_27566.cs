using System;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_27566 : RavenTestBase
    {
        public RavenDB_27566(ITestOutputHelper output) : base(output)
        {
        }

        // archived documents are stored compressed regardless of the collection's compression configuration, also after a regular put
        [RavenFact(RavenTestCategory.ExpirationRefresh | RavenTestCategory.Compression)]
        public async Task ArchivedDocumentModifiedByRegularPut_MustStayCompressed()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            await DataArchivalHelper.SetupDataArchival(store, Server.ServerStore, new DataArchivalConfiguration { Disabled = false, ArchiveFrequencyInSec = 100_000 });

            using (var session = store.OpenAsyncSession())
            {
                var item = new Item { Name = new string('a', 2048) };
                await session.StoreAsync(item, "items/1");
                session.Advanced.GetMetadataFor(item)[Constants.Documents.Metadata.ArchiveAt] = DateTime.UtcNow.AddDays(-1).ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.SaveChangesAsync();
            }

            await database.DataArchivist.ArchiveDocs();
            Assert.True(IsArchivedAndCompressed(database, "items/1"));

            using (var session = store.OpenAsyncSession())
            {
                var item = await session.LoadAsync<Item>("items/1");
                item.Name += "b";
                await session.SaveChangesAsync();
            }

            Assert.True(IsArchivedAndCompressed(database, "items/1"));
        }

        private static bool IsArchivedAndCompressed(DocumentDatabase database, string id)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            using (var doc = database.DocumentsStorage.Get(context, id))
            {
                Assert.True(doc.Flags.Contain(DocumentFlags.Archived));
                var table = context.Transaction.InnerTransaction.OpenTable(database.DocumentsStorage.DocsSchema, new CollectionName("Items").GetTableName(CollectionTableType.Documents));
                return table.GetInfoFor(doc.StorageId).IsCompressed;
            }
        }

        private class Item
        {
            public string Name { get; set; }
        }
    }
}
