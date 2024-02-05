using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_21242 : ClusterTestBase
    {
        public RavenDB_21242(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task ShouldValidateUnusedIds()
        {
            var (nodes, leader) = await CreateRaftCluster(3);
            using var store = GetDocumentStore(new Options()
            {
                Server = leader,
                ReplicationFactor = 3
            });
            var database = store.Database;

            string deletedDbId = string.Empty;

            foreach (var node in nodes)
            {
                var dbId = await GetDbId(node, database);

                if (node.ServerStore.NodeTag == "B")
                {
                    deletedDbId = dbId;
                    continue;
                }

                var cmd = new UpdateUnusedDatabasesOperation(store.Database,
                    new HashSet<string> { dbId }, validate: true);

                var e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.Server.SendAsync(cmd));
                Assert.True(e.InnerException is InvalidOperationException);
                Assert.Contains($"'{dbId}' cannot be added to the 'unused ids' list (of '{database}'), because it's the database id of '{database}' on node {node.ServerStore.NodeTag}.", e.InnerException.Message);
            }

            // delete database from leader node
            await store.Maintenance.Server.SendAsync(
                new DeleteDatabasesOperation(store.Database, true, "B", TimeSpan.FromSeconds(30)));

            using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var rawRecord = leader.ServerStore.Cluster.ReadRawDatabaseRecord(context, database))
            {
                var topology = rawRecord.Topology;

                var cmd = new UpdateUnusedDatabasesOperation(store.Database,
                    new HashSet<string> { topology.DatabaseTopologyIdBase64 }, validate: true);

                var e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.Server.SendAsync(cmd));
                Assert.True(e.InnerException is InvalidOperationException);
                Assert.Contains($"'DatabaseTopologyIdBase64' ({topology.DatabaseTopologyIdBase64}) cannot be added to the 'unused ids' list (of '{database}')", e.InnerException.Message);

                cmd = new UpdateUnusedDatabasesOperation(store.Database,
                    new HashSet<string> { topology.ClusterTransactionIdBase64 }, validate: true);

                e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.Server.SendAsync(cmd));
                Assert.True(e.InnerException is InvalidOperationException);
                Assert.Contains($"'ClusterTransactionIdBase64' ({topology.ClusterTransactionIdBase64}) cannot be added to the 'unused ids' list (of '{database}')", e.InnerException.Message);
            }

            Assert.True(await WaitForValueAsync(async () =>
            {
                var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                return res != null && res.Topology.Count == 2;
            }, true));

            await store.Maintenance.Server.SendAsync(new UpdateUnusedDatabasesOperation(store.Database,
                new HashSet<string> { deletedDbId }, validate: true));

            var cmd1 = new UpdateUnusedDatabasesOperation(store.Database,
                new HashSet<string> { "6ZY2cIMkCEOzFD3CtbdH1@" }, validate: true); // @ is forbidden char

            var ex = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.Server.SendAsync(cmd1));
            Assert.True(ex.InnerException is InvalidOperationException);
            Assert.Contains("Database ID '6ZY2cIMkCEOzFD3CtbdH1@' isn't valid because it isn't Base64Id (it contains chars which cannot be in Base64String)", ex.InnerException.Message);

            cmd1 = new UpdateUnusedDatabasesOperation(store.Database,
                new HashSet<string> { "6ZY2cIMkCEOzFD3CtbdH1AAA" }, validate: true);

            ex = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.Server.SendAsync(cmd1));
            Assert.True(ex.InnerException is InvalidOperationException);
            Assert.Contains("Database ID '6ZY2cIMkCEOzFD3CtbdH1AAA' isn't valid because its length (24) isn't 22", ex.InnerException.Message);

            cmd1 = new UpdateUnusedDatabasesOperation(store.Database,
                new HashSet<string> { "6ZY2cIMkCEOzFD3CtbdH1+" }, validate: true);

            await store.Maintenance.Server.SendAsync(cmd1);
        }

        private static async Task<string> GetDbId(RavenServer ravenServer, string database)
        {
            var db = await ravenServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
            return db.DocumentsStorage.Environment.Base64Id;
        }
    }
}
