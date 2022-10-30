using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Replication
{
    public class ShardedExternalReplicationTests : ReplicationTestBase
    {
        public ShardedExternalReplicationTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ServerWideExternalReplicationShouldWork_NonShardedToSharded()
        {
            var clusterSize = 3;
            var dbName = GetDatabaseName();

            var (_, leader) = await CreateRaftCluster(clusterSize);
            var (shardNodes, shardsLeader) = await CreateRaftCluster(clusterSize);

            await CreateDatabaseInCluster(dbName, 3, leader.WebUrl);
            await ShardingCluster.CreateShardedDatabaseInCluster(dbName, 3, (shardNodes, shardsLeader));

            using (var store = new DocumentStore() { Urls = new[] { leader.WebUrl }, Database = dbName }.Initialize())
            {
                var putConfiguration = new ServerWideExternalReplication
                {
                    MentorNode = leader.ServerStore.NodeTag,
                    TopologyDiscoveryUrls = shardNodes.Select(s => s.WebUrl).ToArray(),
                    Name = dbName
                };

                var result = await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(putConfiguration));
                var serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideExternalReplicationOperation(result.Name));
                Assert.NotNull(serverWideConfiguration);

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));
                Assert.Equal(1, record.ExternalReplications.Count);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    shardNodes,
                    dbName,
                    "users/1",
                    u => u.Name.Equals("Karmel"),
                    TimeSpan.FromSeconds(60)));
            }
        }

        [Fact]
        public async Task ServerWideExternalReplicationShouldWork_ShardedToNonSharded()
        {
            var clusterSize = 3;
            var dbName = GetDatabaseName();

            var (nodes, leader) = await CreateRaftCluster(clusterSize);
            var (shardNodes, shardsLeader) = await CreateRaftCluster(clusterSize);

            await CreateDatabaseInCluster(dbName, 3, leader.WebUrl);
            await ShardingCluster.CreateShardedDatabaseInCluster(dbName, 3, (shardNodes, shardsLeader));

            using (var store = new DocumentStore() { Urls = new[] { shardsLeader.WebUrl }, Database = dbName }.Initialize())
            {
                var putConfiguration = new ServerWideExternalReplication
                {
                    MentorNode = shardsLeader.ServerStore.NodeTag,
                    TopologyDiscoveryUrls = nodes.Select(s => s.WebUrl).ToArray(),
                    Name = dbName
                };

                var result = await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(putConfiguration));
                var serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideExternalReplicationOperation(result.Name));
                Assert.NotNull(serverWideConfiguration);

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));
                Assert.Equal(1, record.ExternalReplications.Count);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    nodes,
                    dbName,
                    "users/1",
                    u => u.Name.Equals("Karmel"),
                    TimeSpan.FromSeconds(60)));
            }
        }
    }
}
