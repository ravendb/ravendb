using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Esprima.Ast;
using FastTests;
using FastTests.Graph;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class EncryptedDatabaseGroup: ClusterTestBase
    {
        [Fact]
        public async Task AddingNodeToEncryptedDatabaseGroupShouldThrow()
        {
            var (nodes, leader) = await CreateRaftClusterWithSsl(3);            

            var option = new Options
            {
                Server = leader,
                ReplicationFactor = 2,
                Encrypted = true
            };

            using (var store = GetDocumentStore(option))
            {
                var res = await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database));
                await WaitForRaftIndexToBeAppliedInCluster(res.RaftCommandIndex, TimeSpan.FromSeconds(15));
                var notInDbGroupServer = Servers.Single(s => res.Topology.Members.Contains(s.ServerStore.NodeTag) == false);
                var dbName = store.Database;
                using (var notInDbGroupStore = GetDocumentStore(new Options
                {
                    Server = notInDbGroupServer,
                    CreateDatabase = false,
                    ModifyDocumentStore = ds => ds.Conventions.DisableTopologyUpdates = true,
                    ClientCertificate = option.ClientCertificate,
                    ModifyDatabaseName = _ => dbName
                }))
                {
                    await Assert.ThrowsAsync<Raven.Client.Exceptions.Database.DatabaseLoadFailureException>(async ()=> await TrySavingDocument(notInDbGroupStore));
                    PutSecrectKeyForDatabaseInServersStore(dbName, notInDbGroupServer);
                    await notInDbGroupServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(dbName, true);
                    await TrySavingDocument(notInDbGroupStore);
                }                
            }
        }

        private static async Task TrySavingDocument(DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User {Name = "Foo"});
                await session.SaveChangesAsync();
            }
        }

        [Fact]
        public async Task DeletingMasterKeyForExistedEncryptedDatabaseShouldFail()
        {
            var (nodes, server) = await CreateRaftClusterWithSsl(1);

            var option = new Options
            {
                Server = server,
                Encrypted = true
            };
            using (var store = GetDocumentStore(option))
            {
                await TrySavingDocument(store);
                using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))                
                {
                    using (ctx.OpenWriteTransaction())
                    {
                        Assert.Throws<InvalidOperationException>(() => server.ServerStore.DeleteSecretKey(ctx, store.Database));
                    }

                    store.Maintenance.Server.Send(new DeleteDatabasesOperation(store.Database, true));
                    using (ctx.OpenWriteTransaction())
                    {
                        server.ServerStore.DeleteSecretKey(ctx, store.Database);
                    }
                }
                
            }
        }
    }
}
