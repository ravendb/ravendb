using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Graph;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class EncryptedDatabaseGroup : ClusterTestBase
    {
        public EncryptedDatabaseGroup(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task AddingNodeToEncryptedDatabaseGroupShouldThrow()
        {
            var (nodes, leader, certificates) = await CreateRaftClusterWithSsl(3);

            EncryptedCluster(nodes, certificates, out var databaseName);

            var options = new Options
            {
                Server = leader,
                ReplicationFactor = 2,
                ClientCertificate = certificates.ClientCertificate1.Value,
                AdminCertificate = certificates.ServerCertificate.Value,
                ModifyDatabaseName = _ => databaseName,
                Encrypted = true
            };

            using (var store = GetDocumentStore(options))
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var notInDbGroupServer = nodes.Single(s => record.Topology.AllNodes.Contains(s.ServerStore.NodeTag) == false);
                DeleteSecretKeyForDatabaseFromServerStore(databaseName, notInDbGroupServer);

                await Assert.ThrowsAsync<RavenException>(async () => await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database)));

                using (var notInDbGroupStore = GetDocumentStore(new Options
                {
                    Server = notInDbGroupServer,
                    CreateDatabase = false,
                    ModifyDocumentStore = ds => ds.Conventions.DisableTopologyUpdates = true,
                    ClientCertificate = options.ClientCertificate,
                    ModifyDatabaseName = _ => databaseName
                }))
                {
                    await Assert.ThrowsAsync<DatabaseLoadFailureException>(async () => await TrySavingDocument(notInDbGroupStore));
                    PutSecretKeyForDatabaseInServerStore(databaseName, notInDbGroupServer);
                    await notInDbGroupServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName, ignoreDisabledDatabase: true);
                    await TrySavingDocument(notInDbGroupStore);
                }
            }
        }

        private static async Task TrySavingDocument(DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Foo" });
                await session.SaveChangesAsync();
            }
        }

        [Fact]
        public async Task DeletingMasterKeyForExistedEncryptedDatabaseShouldFail()
        {
            EncryptedServer(out var certificates, out var databaseName);

            var options = new Options
            {
                ModifyDatabaseName = _ => databaseName,
                ClientCertificate = certificates.ServerCertificate.Value,
                AdminCertificate = certificates.ServerCertificate.Value,
                Encrypted = true
            };

            using (var store = GetDocumentStore(options))
            {
                await TrySavingDocument(store);
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                {
                    using (ctx.OpenWriteTransaction())
                    {
                        Assert.Throws<InvalidOperationException>(() => Server.ServerStore.DeleteSecretKey(ctx, store.Database));
                    }

                    store.Maintenance.Server.Send(new DeleteDatabasesOperation(store.Database, true));
                    using (ctx.OpenWriteTransaction())
                    {
                        Server.ServerStore.DeleteSecretKey(ctx, store.Database);
                    }
                }
            }
        }

        [Fact]
        public async Task DeletingEncryptedDatabaseFromDatabaseGroup()
        {
            var (nodes, server, certificates) = await CreateRaftClusterWithSsl(3);

            EncryptedCluster(nodes, certificates, out var databaseName);

            var options = new Options
            {
                Server = server,
                ReplicationFactor = 3,
                Encrypted = true,
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ClientCertificate1.Value,
                ModifyDatabaseName = _ => databaseName
            };

            using (var store = GetDocumentStore(options))
            {
                await TrySavingDocument(store);
                store.Maintenance.Server.Send(new DeleteDatabasesOperation(store.Database, true, fromNode: nodes[0].ServerStore.NodeTag));
                var value = WaitForValue(() => GetTopology().AllNodes.Count(), 2);
                Assert.Equal(2, value);

                store.Maintenance.Server.Send(new DeleteDatabasesOperation(store.Database, true, fromNode: nodes[1].ServerStore.NodeTag));
                value = WaitForValue(() => GetTopology().AllNodes.Count(), 1);
                Assert.Equal(1, value);

                DatabaseTopology GetTopology()
                {
                    var serverStore = nodes[2].ServerStore;
                    using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    using (var databaseRecord = serverStore.Cluster.ReadRawDatabaseRecord(ctx, store.Database))
                    {
                        return databaseRecord.Topology;
                    }
                }
            }
        }
    }
}
