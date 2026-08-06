using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents
{
    public class DocumentsTransactionCacheTests : RavenTestBase
    {
        public DocumentsTransactionCacheTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Core | RavenTestCategory.Compression)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TransactionCacheShouldMatchStorageAfterEveryKindOfCollectionChange(bool compressed)
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    if (compressed)
                        record.DocumentsCompression = new DocumentsCompressionConfiguration(compressRevisions: false, compressAllCollections: true);
                }
            }))
            {
                const int numberOfCollections = 20;
                var database = await GetDatabase(store.Database);

                using (var commands = store.Commands())
                {
                    // puts across multiple collections
                    for (var c = 0; c < numberOfCollections; c++)
                    {
                        for (var i = 0; i < 3; i++)
                        {
                            commands.Put($"docs{c}/{i}", null, new { Data = $"initial {c}/{i}" }, CollectionMetadata(c));
                        }
                    }

                    AssertCacheMatchesStorage(database);

                    // updates
                    for (var c = 0; c < 5; c++)
                    {
                        commands.Put($"docs{c}/0", null, new { Data = "updated" }, CollectionMetadata(c));
                    }

                    AssertCacheMatchesStorage(database);

                    // deletes create tombstones
                    for (var c = 5; c < 10; c++)
                    {
                        commands.Delete($"docs{c}/1", null);
                    }

                    AssertCacheMatchesStorage(database);

                    // empty one collection completely
                    for (var i = 0; i < 3; i++)
                    {
                        commands.Delete($"docs10/{i}", null);
                    }

                    AssertCacheMatchesStorage(database);

                    // purge the tombstones
                    await database.TombstoneCleaner.ExecuteCleanup();

                    AssertCacheMatchesStorage(database);

                    // a brand new collection
                    commands.Put("late/1", null, new { Data = "late" }, new Dictionary<string, object>
                    {
                        { Constants.Documents.Metadata.Collection, "LateCollection" }
                    });

                    AssertCacheMatchesStorage(database);
                }

                // concurrent writes drive the transaction merger into async commits,
                // where the next write transaction starts before the previous one published its cache
                var tasks = Enumerable.Range(0, 200).Select(i => Task.Run(() =>
                {
                    using (var commands = store.Commands())
                    {
                        commands.Put($"parallel/{i}", null, new { Data = i }, new Dictionary<string, object>
                        {
                            { Constants.Documents.Metadata.Collection, $"Parallel{i % 10}" }
                        });
                    }
                })).ToArray();

                await Task.WhenAll(tasks);

                AssertCacheMatchesStorage(database);
            }
        }

        private static Dictionary<string, object> CollectionMetadata(int i)
        {
            return new Dictionary<string, object>
            {
                { Constants.Documents.Metadata.Collection, $"Docs{i}" }
            };
        }

        private static void AssertCacheMatchesStorage(DocumentDatabase database)
        {
            var storage = database.DocumentsStorage;

            using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readContext))
            using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext writeContext))
            using (readContext.OpenReadTransaction())
            using (writeContext.OpenWriteTransaction())
            {
                // write transactions never use the cache, so they provide the values read directly from storage
                var readTx = readContext.Transaction.InnerTransaction;
                var writeTx = writeContext.Transaction.InnerTransaction;

                Assert.False(readTx.IsWriteTransaction);
                Assert.True(writeTx.IsWriteTransaction);

                Assert.Equal(DocumentsStorage.ReadLastDocumentEtag(writeTx), DocumentsStorage.ReadLastDocumentEtag(readTx));
                Assert.Equal(DocumentsStorage.ReadLastTombstoneEtag(writeTx), DocumentsStorage.ReadLastTombstoneEtag(readTx));
                Assert.Equal(storage.ReadLastEtag(writeTx), storage.ReadLastEtag(readTx));

                var collections = storage.GetCollections(writeContext).Select(x => x.Name).ToList();
                Assert.NotEmpty(collections);

                foreach (var collection in collections)
                {
                    Assert.Equal(storage.GetLastDocumentEtag(writeTx, collection), storage.GetLastDocumentEtag(readTx, collection));
                    Assert.Equal(storage.GetLastTombstoneEtag(writeTx, collection), storage.GetLastTombstoneEtag(readTx, collection));
                    Assert.Equal(storage.GetLastDocumentChangeVector(writeTx, collection), storage.GetLastDocumentChangeVector(readTx, collection));
                }
            }
        }
    }
}
