using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Monitoring.Snmp;
using Raven.Server.Monitoring.Snmp.Objects.Database;
using Raven.Server.ServerWide;
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

            // WaitForUserToContinueTheTest(store);

            var unusedIds = new List<string>();

            foreach (var node in nodes)
            {
                var dbId = await GetDbId(node, database);
                unusedIds.Add(dbId);
            }
            using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var rawRecord = leader.ServerStore.Cluster.ReadRawDatabaseRecord(context, database))
            {
                var topology = rawRecord.Topology;
                unusedIds.Add(topology.DatabaseTopologyIdBase64);
                unusedIds.Add(topology.ClusterTransactionIdBase64);
            }

            foreach(var dbId in unusedIds)
            {
                var cmd = new UpdateUnusedDatabasesOperation(store.Database,
                    new HashSet<string> { dbId }, validate: true);

                var e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.Server.SendAsync(cmd));
                Assert.True(e.InnerException is InvalidOperationException);
            }
        }

        private async Task<string> GetDbId(RavenServer ravenServer, string database)
        {
            var db = await ravenServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
            return db.DocumentsStorage.Environment.Base64Id;
        }
    }
}
