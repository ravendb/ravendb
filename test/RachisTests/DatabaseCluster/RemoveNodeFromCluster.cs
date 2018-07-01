using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Rachis;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace RachisTests.DatabaseCluster
{
    public class RemoveNodeFromCluster : ClusterTestBase
    {
        [Fact]
        public async Task RemovedNodeChangeReplicationFactor()
        {
            var dbName = GetDatabaseName();
            await RemoveNodeWithDatabase(dbName, 5, 5);
        }

        [Fact]
        public async Task ReconnectRemovedNodeWithDatabases()
        {
            var dbName = GetDatabaseName();
            var removed = await RemoveNodeWithDatabase(dbName, 5, 5);

            RavenServer leaderNode;
            using (var store = new DocumentStore
            {
                Urls = new[] { removed.WebUrl },
                Database = dbName,
            }.Initialize())
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));
                Assert.Equal(5, record.Topology.Count);
                Assert.Equal(5, record.Topology.ReplicationFactor);

                //reconnect the removed node to the original cluster
                leaderNode = await ActionWithLeader(leader => leader.ServerStore.AddNodeToClusterAsync(removed.WebUrl, removed.ServerStore.NodeTag));
                Assert.True(await removed.ServerStore.WaitForState(RachisState.Follower, CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(30)),
                    "Removed node wasn't reconnected with the cluster.");
                await removed.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, leaderNode.ServerStore.LastRaftCommitIndex);
                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));
                
                Assert.Equal(4, record.Topology.Count);
                Assert.Equal(4, record.Topology.ReplicationFactor);
            }

            using (var store = new DocumentStore
            {
                Urls = new[] { leaderNode.WebUrl },
                Database = dbName,
            }.Initialize())
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));
                Assert.Equal(4, record.Topology.Count);
                Assert.Equal(4, record.Topology.ReplicationFactor);
            }
        }

        [Fact]
        public async Task BootstrapRemovedNode()
        {
            var dbName = GetDatabaseName();
            var removed = await RemoveNodeWithDatabase(dbName, 5, 5);

            using (var store = new DocumentStore
            {
                Urls = new[] { removed.WebUrl },
                Database = dbName,
            }.Initialize())
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));
                Assert.Equal(5, record.Topology.Count);
                Assert.Equal(5, record.Topology.ReplicationFactor);

                //bootstrap the removed node to a single-node cluster
                removed.ServerStore.EnsureNotPassive();
                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));
                Assert.Equal(1, record.Topology.Count);
                Assert.Equal(1, record.Topology.ReplicationFactor);

                Assert.True(WaitForDocument(store, "foo/bar"));
            }
        }

        [Fact]
        public async Task ReconnectRemovedNodeWithOneDatabase()
        {
            // BAD IDEA - we lose the database!
            var dbName = GetDatabaseName();
            var removed = await RemoveNodeWithDatabase(dbName, 5, 1);

            using (var store = new DocumentStore
            {
                Urls = new[] { removed.WebUrl },
                Database = dbName,
            }.Initialize())
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));
                Assert.Equal(1, record.Topology.Count);
                Assert.Equal(1, record.Topology.ReplicationFactor);

                //reconnect the removed node to the original cluster
                var leaderNode = await ActionWithLeader(leader => leader.ServerStore.AddNodeToClusterAsync(removed.WebUrl, removed.ServerStore.NodeTag));
                Assert.True(await removed.ServerStore.WaitForState(RachisState.Follower, CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(30)),
                    "Removed node wasn't reconnected with the cluster.");
                await removed.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, leaderNode.ServerStore.LastRaftCommitIndex);
                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));
                Assert.Null(record);
            }
        }

        [Fact]
        public async Task BootstrapRemovedNodeWithOneDatabase()
        {
            var dbName = GetDatabaseName();
            var removed = await RemoveNodeWithDatabase(dbName, 5, 1);

            using (var store = new DocumentStore
            {
                Urls = new[] { removed.WebUrl },
                Database = dbName,
            }.Initialize())
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));
                Assert.Equal(1, record.Topology.Count);
                Assert.Equal(1, record.Topology.ReplicationFactor);

                //bootstrap the removed node to a single-node cluster
                removed.ServerStore.EnsureNotPassive();
                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));
                Assert.Equal(1, record.Topology.Count);
                Assert.Equal(1, record.Topology.ReplicationFactor);

                Assert.True(WaitForDocument(store, "foo/bar"));
            }
        }

        [InlineData(3)]
        [InlineData(5)]
        [Theory]
        public async Task RemovedLeaderCauseReelection(int numberOfNodes)
        {
            var leader = await CreateRaftClusterAndGetLeader(numberOfNodes);
            using (var cts = new CancellationTokenSource())
            {
                try
                {
                    var followerTasks = Servers.Where(s => s != leader).Select(s => s.ServerStore.WaitForState(RachisState.Leader, cts.Token));
                    await ActionWithLeader(l => l.ServerStore.RemoveFromClusterAsync(leader.ServerStore.NodeTag));
                    Assert.True(await Task.WhenAny(followerTasks).WaitAsync(TimeSpan.FromSeconds(30)));
                }
                finally
                {
                    cts.Cancel();
                }
            }
        }

        private async Task<RavenServer> RemoveNodeWithDatabase(string dbName, int nodesAmount, int replicationFactor)
        {
            var firstLeader = await CreateRaftClusterAndGetLeader(nodesAmount, leaderIndex: 0);
            var (_, servers) = await CreateDatabaseInCluster(dbName, replicationFactor, firstLeader.WebUrl);
            var removed = servers.Last();
            using (var store = new DocumentStore
            {
                Urls = new[] { firstLeader.WebUrl },
                Database = dbName,
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(TimeSpan.FromSeconds(30), replicas: replicationFactor - 1);
                    await session.StoreAsync(new User { Name = "Karmel" }, "foo/bar");
                    await session.SaveChangesAsync();
                }

                await ActionWithLeader(l => l.ServerStore.RemoveFromClusterAsync(removed.ServerStore.NodeTag));
                Assert.True(await removed.ServerStore.WaitForState(RachisState.Passive, CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(30)),
                    "Removed node wasn't move to passive state.");

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));

                if (removed.WebUrl == firstLeader.WebUrl)
                {
                    Assert.Equal(replicationFactor, record.Topology.Count);
                    Assert.Equal(replicationFactor, record.Topology.ReplicationFactor);
                    return removed;
                }

                if (replicationFactor == 1)
                {
                    // if we remove the only node that have the database, it should delete the record in the cluster.
                    Assert.Null(record);
                    return removed;
                }

                Assert.Equal(replicationFactor - 1, record.Topology.Count);
                Assert.Equal(replicationFactor - 1, record.Topology.ReplicationFactor);
            }
            return removed;
        }
    }
}
