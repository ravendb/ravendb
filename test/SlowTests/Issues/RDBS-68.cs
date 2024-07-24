using System.Collections.Generic;
using System.Threading;
using Xunit.Abstractions;
using Xunit;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Voron;

namespace SlowTests.Issues
{
    public class RDBS_68 : RavenTestBase
    {
        public RDBS_68(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanDeleteAllExpiredDocsAfterRunningTheExpirationBundle()
        {
            using (var store = GetDocumentStore())
            {
                var config = new ExpirationConfiguration
                {
                    Disabled = false,
                    DeleteFrequencyInSec = 1000
                };

                await ExpirationHelper.SetupExpiration(store, Server.ServerStore, config);

                var expiry = SystemTime.UtcNow.AddMinutes(-5);

                var deleteList = new List<string>();

                var batchSize = 10;
                var batches = 5;
                for (var i = 0; i < batches; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        for (var j = 0; j < batchSize; j++)
                        {
                            var item = new Item();
                            await session.StoreAsync(item);
                            deleteList.Add(item.Id);
                            var metadata = session.Advanced.GetMetadataFor(item);
                            metadata[Constants.Documents.Metadata.Expires] = expiry.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                        }

                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    foreach (var toDelete in deleteList)
                    {
                        session.Delete(toDelete);
                    }

                    await session.SaveChangesAsync();
                }

                string lastId;
                using (var session = store.OpenAsyncSession())
                {
                    var item = new Item();
                    await session.StoreAsync(item);
                    lastId = item.Id;
                    var metadata = session.Advanced.GetMetadataFor(item);
                    metadata[Constants.Documents.Metadata.Expires] = SystemTime.UtcNow.AddMinutes(-1).ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);

                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                ValidateTotalExpirationCount(batchSize);

                var expiredDocumentsCleaner = database.ExpiredDocumentsCleaner;
                await expiredDocumentsCleaner.CleanupExpiredDocs(batchSize);

                ValidateTotalExpirationCount(0);

                using (var session = store.OpenAsyncSession())
                {
                    var item = await session.LoadAsync<Item>(lastId);
                    Assert.Null(item);
                }

                void ValidateTotalExpirationCount(int expected)
                {
                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var currentTime = database.Time.GetUtcNow();

                        DatabaseRecord dbRecord;
                        string nodeTag;
                        
                        using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
                        using (serverContext.OpenReadTransaction())
                        {
                            dbRecord = database.ServerStore.Cluster.ReadDatabase(serverContext, database.Name);
                            nodeTag = database.ServerStore.NodeTag;
                        }
                        
                        var options = new BackgroundWorkParameters(context, currentTime, dbRecord, nodeTag, batchSize);
                        var totalCount = 0;
                        var expired = database.DocumentsStorage.ExpirationStorage.GetDocuments(options, ref totalCount, out _, CancellationToken.None);
                        Assert.Equal(expected, totalCount);
                    }
                }
            }
        }

        private class Item
        {
            public string Id { get; set; }
        }
    }
}
