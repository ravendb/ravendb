using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide.Context;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Indexing.MapReduce
{
    public class OutputReduceToCollectionReplicationTests : ReplicationTestBase, ITombstoneAware
    {
        public OutputReduceToCollectionReplicationTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ReduceOutputShouldNotBeReplicated()
        {
            var date = new DateTime(2017, 2, 14);
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await SetupReplicationAsync(store1, store2);
                await store1.ExecuteIndexAsync(new OutputReduceToCollectionTests.DailyInvoicesIndex());

                using (var session = store1.OpenAsyncSession())
                {
                    for (int i = 0; i < 30; i++)
                    {
                        await session.StoreAsync(new OutputReduceToCollectionTests.Invoice { Amount = 1, IssuedAt = date.AddHours(i * 6) });
                    }
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store1);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new OutputReduceToCollectionTests.Marker { Name = "Marker" }, "marker");
                    await session.SaveChangesAsync();
                }
                Assert.True(WaitForDocument(store2, "marker"));

                var collectionStatistics = await store1.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
                Assert.Equal(8, collectionStatistics.Collections["DailyInvoices"]);
                Assert.Equal(30, collectionStatistics.Collections["Invoices"]);

                collectionStatistics = await store2.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
                Assert.Equal(30, collectionStatistics.Collections["Invoices"]);
                Assert.False(collectionStatistics.Collections.ContainsKey("DailyInvoices"));
                Assert.Equal(32, collectionStatistics.CountOfDocuments);

                // Check that we do not replicate tombstones of artificial documents
                var database = await Databases.GetDocumentDatabaseInstanceFor(store1);
                var database2 = await Databases.GetDocumentDatabaseInstanceFor(store2);
                database.TombstoneCleaner.Subscribe(this);
                database2.TombstoneCleaner.Subscribe(this);

                var operation = await store1.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery { Query = "FROM Invoices"}));
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new OutputReduceToCollectionTests.Marker { Name = "Marker 2" }, "marker2");
                    await session.SaveChangesAsync();
                }
                Indexes.WaitForIndexing(store1);
                Assert.True(WaitForDocument(store2, "marker2"));

                using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                using (context.OpenReadTransaction())
                {
                    var dailyInvoicesTombstones = database.DocumentsStorage.GetTombstonesFrom(context, "DailyInvoices", 0, 0, 128).Count();
                    Assert.Equal(8, dailyInvoicesTombstones);
                }

                using (var context = DocumentsOperationContext.ShortTermSingleUse(database2))
                using (var tx = context.OpenReadTransaction())
                {
                    var dailyInvoicesTombstones = database.DocumentsStorage.GetTombstonesFrom(context, "DailyInvoices", 0, 0, 128).Count();
                    Assert.Equal(0, dailyInvoicesTombstones);
                    var collections = database.DocumentsStorage.GetTombstoneCollections(tx.InnerTransaction).ToList();
                    Assert.Equal(5, collections.Count);
                    Assert.DoesNotContain("DailyInvoices", collections, StringComparer.OrdinalIgnoreCase);
                }
            }
        }

        public string TombstoneCleanerIdentifier => nameof(OutputReduceToCollectionReplicationTests);

        public Dictionary<string, LastTombstoneInfo> GetLastProcessedTombstonesPerCollection(ITombstoneAware.TombstoneType tombstoneType)
        {
            return new Dictionary<string, LastTombstoneInfo>
            {
                ["DailyInvoices"] = new LastTombstoneInfo("", "DailyInvoices", 0)
            };
        }

        public Dictionary<TombstoneDeletionBlockageSource, HashSet<string>> GetDisabledSubscribersCollections(HashSet<string> tombstoneCollections)
        {
            throw new NotImplementedException();
        }
    }
}
