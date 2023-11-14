using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_21150 : ClusterTestBase
    {
        public RavenDB_21150(ITestOutputHelper output) : base(output)
        {

        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Replication)]
        public async Task DontDeleteNonReplicatedTombstone()
        {
            var (nodes, leader) = await CreateRaftCluster(3);
            var databaseName = GetDatabaseName();
            var nodeToRemove = leader.WebUrl == nodes[2].WebUrl ? nodes[0] : nodes[2];
            using (var store1 = GetDocumentStore(new Options
            {
                   ModifyDatabaseName = s => databaseName,
                   ReplicationFactor = 3,
                   Server = leader,
                   RunInMemory = false,
                   ModifyDocumentStore = (store) => store.Conventions.DisableTopologyUpdates = true
            }))
            using (var store2 = new DocumentStore
            {
                   Database = databaseName,
                   Urls = new []{ nodes.First(x => x.WebUrl != leader.WebUrl && x.WebUrl != nodeToRemove.WebUrl).WebUrl},
                   Conventions = new DocumentConventions()
                   {
                       DisableTopologyUpdates = true
                   }
            }.Initialize())
            {
                using (var session = store1.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(TimeSpan.FromSeconds(30), replicas: 2);
                    await session.StoreAsync(new User { Name = "Toli" }, "users/1");
                    await session.SaveChangesAsync();
                }

                await DisposeServerAndWaitForFinishOfDisposalAsync(nodeToRemove);

                using (var session = store1.OpenAsyncSession())
                {
                    session.Delete( "users/1");
                    await session.SaveChangesAsync();
                }

                Assert.True(WaitForDocumentDeletion(store1, "users/1", 15000));
                Assert.True(WaitForDocumentDeletion(store2, "users/1", 15000));
                DocumentDatabase documentDatabase;
                nodes.Remove(nodeToRemove);

                foreach (var node in nodes)
                {
                    documentDatabase = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                    using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        Assert.Equal(1, documentDatabase.DocumentsStorage.GetNumberOfTombstones(ctx));
                    }
                }

                foreach (var node in nodes)
                {
                    await node.ServerStore.DatabasesLandlord.RestartDatabase(databaseName);
                }

                int desCount;
                await WaitForValueAsync(async () =>
                {
                    desCount = 0;
                    foreach (var node in nodes)
                    {
                        documentDatabase = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                        desCount += documentDatabase.ReplicationLoader.Destinations.Count;
                    }
                    return desCount;
                }, 3, 90000);

                foreach (var node in nodes)
                {
                    documentDatabase = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database);
                    long count = await documentDatabase.TombstoneCleaner.ExecuteCleanup();
                    Assert.Equal(0, count);
                }

                foreach (var node in nodes)
                {
                    documentDatabase = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database);
                    using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var numberOfTombstone = documentDatabase.DocumentsStorage.GetNumberOfTombstones(ctx);
                        Assert.Equal(1, numberOfTombstone);
                    }
                }
            }
        }
    }
}
