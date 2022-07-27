using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Cluster
{
    public class NodeRedistribution: ClusterTestBase
    {
        public NodeRedistribution(ITestOutputHelper output) : base(output)
        {
        }

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
                store.Maintenance.Server.Send(new SetDatabaseDynamicDistributionOperation(store.Database, true));
                await DisposeAndRemoveServer(res.Servers.First());
                var wait = (moveToRehabGraceTime + replicaTimeout)* 10 * 1000;
                if (Debugger.IsAttached)
                {
                    wait *= 10;
                }
                DatabaseTopology dbToplogy;
                var condition = SpinWait.SpinUntil(() =>
                    {
                        dbToplogy = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(databaseName)).Topology;
                        return dbToplogy.AllNodes.Contains(notInDbGroup.ServerStore.NodeTag);
                    }, wait);                
                Assert.True(condition, $"Waited too long ({wait}ms)for database to move to a new node");
            }

        }
    }
}
