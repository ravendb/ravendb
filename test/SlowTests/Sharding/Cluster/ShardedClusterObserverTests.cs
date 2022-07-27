using System.Linq;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Cluster
{
    public class ShardedClusterObserverTests : ClusterTestBase
    {
        public ShardedClusterObserverTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task CanMoveToRehab()
        {
            var database = GetDatabaseName();
            var cluster = await CreateRaftCluster(3, watcherCluster: true, leaderIndex: 0);
            await ShardingCluster.CreateShardedDatabaseInCluster(database, replicationFactor: 3, cluster, shards: 3);
            await DisposeServerAndWaitForFinishOfDisposalAsync(cluster.Nodes[1]);

            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                CreateDatabase = false,
                ModifyDatabaseName = _ => database
            }))
            {
                await AssertWaitForValueAsync(async () =>
                {
                    var shards = await ShardingCluster.GetShards(store);
                    return shards.Sum(s => s.Rehabs.Count);
                }, 3);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task CanAddNodeToShard()
        {
            var database = GetDatabaseName();
            var cluster = await CreateRaftCluster(3, watcherCluster: true, leaderIndex: 0);
            await ShardingCluster.CreateShardedDatabaseInCluster(database, replicationFactor: 1, cluster, shards: 3);

            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                CreateDatabase = false,
                ModifyDatabaseName = _ => database
            }))
            {
                for (int i = 0; i < 3; i++)
                {
                    var add = new AddDatabaseNodeOperation(database, shard: i);
                    await store.Maintenance.Server.SendAsync(add);
                }

                await AssertWaitForValueAsync(async () =>
                {
                    var shards = await ShardingCluster.GetShards(store);
                    return shards.Sum(s => s.Members.Count);
                }, 6);
            }
        }
    }
}
