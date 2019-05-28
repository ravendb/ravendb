using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Cluster
{
    public class NodeRedistribution: ClusterTestBase
    {
        [Fact]
        public async Task CanRedistributeDatabaseWhenNodeGoesDown()
        {
            var databaseName = "NodeRedistribution";
            var replicaTimeout = 4;
            var moveToRehabGraceTime = 4;
            var cluster = await CreateRaftCluster(3, true, 0, customSettings: new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = moveToRehabGraceTime.ToString(),
                [RavenConfiguration.GetKey(x => x.Cluster.AddReplicaTimeout)] = replicaTimeout.ToString()
            });
            var res = await CreateDatabaseInCluster(databaseName, 2, cluster.Leader.WebUrl);
            var notInDbGroup = cluster.Nodes.First(s => res.Servers.Contains(s) == false);
            using (var store = new DocumentStore
            {
                Database = databaseName,
                Urls = new[] { cluster.Leader.WebUrl }
            }.Initialize())
            {
                store.Maintenance.Server.Send(new SetDatabaseDynamicDistribution(store.Database, true));
                await DisposeAndRemoveServer(res.Servers.First());     
                Thread.Sleep((moveToRehabGraceTime + replicaTimeout)* 2 * 1000);
                var dbToplogy = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;
                Assert.True(dbToplogy.AllNodes.Contains(notInDbGroup.ServerStore.NodeTag));
            }

        }
    }
}
