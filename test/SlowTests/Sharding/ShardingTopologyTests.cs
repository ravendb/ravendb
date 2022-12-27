using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NuGet.Packaging;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding
{
    public class ShardingTopologyTests : ClusterTestBase
    {
        public ShardingTopologyTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task EnsureCantDeleteShardFromDatabaseWhileItHasBuckets()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(leader, shards: 3, shardReplicationFactor: 2, orchestratorReplicationFactor: 2);

            using (var store = GetDocumentStore(options))
            {
                //add doc to some shard to ensure it has a bucket
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                var shardToDelete = await Sharding.GetShardNumber(store, "users/1");

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var nodesContainingNewShard = record.Sharding.Shards[shardToDelete].Members;

                //try to delete shard while it has bucket ranges mapping

                var deleteShardDatabaseRes = store.Maintenance.Server.Send(new DeleteDatabasesOperation(store.Database, shardToDelete, hardDelete: true, fromNode: nodesContainingNewShard[0]));
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(deleteShardDatabaseRes.RaftCommandIndex);

                var error = await Assert.ThrowsAsync<RavenException>(async () =>
                {
                    //delete shard's databases all on all nodes
                    deleteShardDatabaseRes = store.Maintenance.Server.Send(new DeleteDatabasesOperation(store.Database, shardToDelete, hardDelete: true, fromNode: nodesContainingNewShard[1]));
                    await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(deleteShardDatabaseRes.RaftCommandIndex);
                });
                Assert.Contains($"it still contains buckets", error.Message);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task AddAndDeleteShardFromDatabase_ShardHasNoBucketsMapping()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(leader, shards: 2, shardReplicationFactor: 2, orchestratorReplicationFactor: 2);

            using (var store = GetDocumentStore(options))
            {
                //add new shard - it will have no buckets mapping
                var addShardRes = store.Maintenance.Server.Send(new AddDatabaseShardOperation(store.Database));
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(addShardRes.RaftCommandIndex);

                var nodesContainingNewShard = addShardRes.ShardTopology.Members;

                //delete shard's databases all on all nodes
                foreach (var node in nodesContainingNewShard)
                {
                    var deleteShardDatabaseRes = store.Maintenance.Server.Send(new DeleteDatabasesOperation(store.Database, addShardRes.ShardNumber, hardDelete: true, fromNode: node));
                    await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(deleteShardDatabaseRes.RaftCommandIndex);
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(2, record.Sharding.Shards.Count);

                //make sure the nodes that held the deleted shard no longer have any of this shard's db instances
                foreach (var node in nodesContainingNewShard)
                {
                    var serverWithNewShard = Servers.Single(x => x.ServerStore.NodeTag == node);
                    Assert.False(serverWithNewShard.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(ShardHelper.ToShardName(store.Database, addShardRes.ShardNumber), out _));
                }
            }
        }

        [Fact]
        public async Task Promote_immediately_should_work()
        {
            var databaseName = GetDatabaseName();
            var (_, leader) = await CreateRaftCluster(3);

            using (var leaderStore = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
            })
            {
                leaderStore.Initialize();

                var (index, dbGroupNodes) = await CreateDatabaseInCluster(databaseName, 2, leader.WebUrl);
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(index, TimeSpan.FromSeconds(30));
                var dbToplogy = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;

                Assert.Equal(2, dbToplogy.AllNodes.Count());
                Assert.Equal(0, dbToplogy.Promotables.Count);

                var nodeNotInDbGroup = Servers.Single(s => dbGroupNodes.Contains(s) == false)?.ServerStore.NodeTag;
                leaderStore.Maintenance.Server.Send(new AddDatabaseNodeOperation(databaseName, nodeNotInDbGroup));
                dbToplogy = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;
                Assert.Equal(3, dbToplogy.AllNodes.Count());
                Assert.Equal(1, dbToplogy.Promotables.Count);
                Assert.Equal(nodeNotInDbGroup, dbToplogy.Promotables[0]);

                await leaderStore.Maintenance.Server.SendAsync(new PromoteDatabaseNodeOperation(databaseName, nodeNotInDbGroup));
                dbToplogy = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;

                Assert.Equal(3, dbToplogy.AllNodes.Count());
                Assert.Equal(0, dbToplogy.Promotables.Count);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task Promote_immediately_sharded()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);

            var options = Sharding.GetOptionsForCluster(leader, shards: 2, shardReplicationFactor: 1, orchestratorReplicationFactor: 2);

            using (var store = GetDocumentStore(options))
            {
                var record = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)));
                var dbTopology = record.Sharding.Orchestrator.Topology;
                Assert.Equal(2, dbTopology.Members.Count);
                Assert.Equal(0, dbTopology.Promotables.Count);

                var allNodes = new[] { "A", "B", "C" };

                var nodeAmount = record.Sharding.Orchestrator.Topology.Members.Count;
                var nodeInOrchestratorTopology = allNodes.First(x => record.Sharding.Orchestrator.Topology.Members.Contains(x));

                //remove the node from orchestrator topology
                store.Maintenance.Server.Send(new RemoveNodeFromOrchestratorTopologyOperation(store.Database, nodeInOrchestratorTopology));
                record = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)));
                dbTopology = record.Sharding.Orchestrator.Topology;
                Assert.Equal(nodeAmount - 1, dbTopology.Members.Count);
                Assert.Equal(0, dbTopology.Promotables.Count);
                Assert.Equal(0, dbTopology.Rehabs.Count);

                leader.ServerStore.Observer.Suspended = true;

                //add node to orchestrator topology
                store.Maintenance.Server.Send(new AddNodeToOrchestratorTopologyOperation(store.Database, nodeInOrchestratorTopology));
                record = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)));
                dbTopology = record.Sharding.Orchestrator.Topology;
                Assert.Equal(nodeAmount - 1, dbTopology.Members.Count);
                Assert.Equal(1, dbTopology.Promotables.Count);
                Assert.Equal(nodeInOrchestratorTopology, dbTopology.Promotables[0]);

                //promote it to orchestrator
                await store.Maintenance.Server.SendAsync(new PromoteDatabaseNodeOperation(store.Database, nodeInOrchestratorTopology));
                record = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)));
                dbTopology = record.Sharding.Orchestrator.Topology;
                Assert.Equal(nodeAmount, dbTopology.Members.Count());
                Assert.Equal(0, dbTopology.Promotables.Count);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task Promote_immediately_for_shard()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(leader, shards: 2, shardReplicationFactor: 1, orchestratorReplicationFactor: 2);

            using (var store = GetDocumentStore(options))
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var shardTopology = record.Sharding.Shards[0];
                Assert.Equal(1, shardTopology.AllNodes.Count());
                Assert.Equal(0, shardTopology.Promotables.Count);
                Assert.Equal(1, shardTopology.ReplicationFactor);

                var nodesInShardTopologies = new HashSet<string>();
                foreach (var shardTop in record.Sharding.Shards)
                {
                    nodesInShardTopologies.AddRange(shardTop.Value.AllNodes);
                }

                var nodeNotContainingShards = nodes.Single(x => nodesInShardTopologies.Contains(x.ServerStore.NodeTag) == false).ServerStore.NodeTag;

                var serverWithNewShard = Servers.Single(x => x.ServerStore.NodeTag == nodeNotContainingShards);
                Assert.False(serverWithNewShard.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(ShardHelper.ToShardName(store.Database, 0), out var _));

                leader.ServerStore.Observer.Suspended = true;

                //duplicate shard to node 0
                var res = store.Maintenance.Server.Send(new AddDatabaseNodeOperation(store.Database, shardNumber: 0, node: nodeNotContainingShards));
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(res.RaftCommandIndex);

                await AssertWaitForValueAsync(async () =>
                {
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    shardTopology = record.Sharding.Shards[0];
                    return shardTopology.Promotables.Count;
                }, 1);

                Assert.Equal(1, shardTopology.Members.Count);
                Assert.Equal(1, shardTopology.Promotables.Count);
                Assert.Equal(nodeNotContainingShards, shardTopology.Promotables[0]);

                //promote immediately
                await store.Maintenance.Server.SendAsync(new PromoteDatabaseNodeOperation(store.Database, 0, nodeNotContainingShards));

                await AssertWaitForValueAsync(async () =>
                {
                    var t = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)));
                    shardTopology = t.Sharding.Shards[0];
                    return shardTopology.Members.Count;
                }, 2);

                Assert.Equal(2, shardTopology.Members.Count);
                Assert.Equal(0, shardTopology.Promotables.Count);
                Assert.Equal(2, shardTopology.ReplicationFactor);

                serverWithNewShard = Servers.Single(x => x.ServerStore.NodeTag == nodeNotContainingShards);
                Assert.True(serverWithNewShard.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(ShardHelper.ToShardName(store.Database, 0), out var _));
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task RemoveShardFromNode()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(leader, shards: 2, shardReplicationFactor: 2, orchestratorReplicationFactor: 2);

            using (var store = GetDocumentStore(options))
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var shardTopology = record.Sharding.Shards[0];
                Assert.Equal(2, shardTopology.Members.Count);
                Assert.Equal(0, shardTopology.Promotables.Count);
                Assert.Equal(2, shardTopology.ReplicationFactor);

                var nodeContainingShard0 = shardTopology.Members.First();

                var serverWithNewShard = Servers.Single(x => x.ServerStore.NodeTag == nodeContainingShard0);
                Assert.True(serverWithNewShard.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(ShardHelper.ToShardName(store.Database, 0), out var _));

                //remove shard from node 0
                var res = store.Maintenance.Server.Send(new DeleteDatabasesOperation(store.Database, shardNumber: 0, hardDelete: true, fromNode: nodeContainingShard0));
                Assert.Equal(1, res.PendingDeletes.Length);
                Assert.True(res.PendingDeletes.Contains(nodeContainingShard0));
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(res.RaftCommandIndex);

                await AssertWaitForValueAsync(async () =>
                {
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    shardTopology = record.Sharding.Shards[0];
                    return shardTopology.Members.Count;
                }, 1);

                Assert.DoesNotContain(nodeContainingShard0, shardTopology.Members);

                serverWithNewShard = Servers.Single(x => x.ServerStore.NodeTag == nodeContainingShard0);
                Assert.False(serverWithNewShard.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(ShardHelper.ToShardName(store.Database, 0), out var _));
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task RemoveNonExistentShardFromNode()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(leader, shards: 2, shardReplicationFactor: 2, orchestratorReplicationFactor: 2);

            using (var store = GetDocumentStore(options))
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var shardTopology = record.Sharding.Shards[0];
                Assert.Equal(2, shardTopology.Members.Count);
                Assert.Equal(0, shardTopology.Promotables.Count);
                Assert.Equal(2, shardTopology.ReplicationFactor);

                //remove non existent shard 5 from node
                var error = Assert.ThrowsAny<RavenException>(() =>
                {
                    store.Maintenance.Server.Send(new DeleteDatabasesOperation(store.Database, shardNumber: 5, hardDelete: true, fromNode: shardTopology.Members[0]));
                });
                Assert.Contains($"Attempting to delete shard database {ShardHelper.ToShardName(store.Database, 5)} but shard 5 doesn't exist for database {store.Database}.", error.Message);

                var record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.NotNull(record2);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task PreventRemovingLastShard()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(leader, shards: 2, shardReplicationFactor: 1, orchestratorReplicationFactor: 2);

            using (var store = GetDocumentStore(options))
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var shardTopology = record.Sharding.Shards[0];
                Assert.Equal(1, shardTopology.Members.Count);
                Assert.Equal(0, shardTopology.Promotables.Count);
                Assert.Equal(1, shardTopology.ReplicationFactor);

                var nodeContainingShard0 = shardTopology.Members.First();

                //remove shard 0 from node
                var error = Assert.ThrowsAny<RavenException>(() =>
                    store.Maintenance.Server.Send(new DeleteDatabasesOperation(store.Database, shardNumber: 0, hardDelete: true, fromNode: nodeContainingShard0)));

                Assert.Contains($"Database {store.Database} cannot be deleted because it is the last copy of shard 0 and it still contains buckets.", error.Message);

                //topology should not change
                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                shardTopology = record.Sharding.Shards[0];
                Assert.Equal(1, shardTopology.Members.Count);
                Assert.Equal(0, shardTopology.Promotables.Count);
                Assert.Equal(1, shardTopology.ReplicationFactor);

                var serverWithShard = Servers.Single(x => x.ServerStore.NodeTag == nodeContainingShard0);
                Assert.True(serverWithShard.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(ShardHelper.ToShardName(store.Database, 0), out var _));
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task CreateWholeShardForDatabase_SpecificShardNumber()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(leader, shards: 2, shardReplicationFactor: 2, orchestratorReplicationFactor: 2);

            using (var store = GetDocumentStore(options))
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var shardTopology = record.Sharding.Shards[0];
                Assert.Equal(2, shardTopology.Members.Count);
                Assert.Equal(0, shardTopology.Promotables.Count);
                Assert.Equal(2, shardTopology.ReplicationFactor);

                foreach (var server in Servers)
                {
                    Assert.False(server.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(ShardHelper.ToShardName(store.Database, 4), out _));
                }

                //create new shard
                var res = store.Maintenance.Server.Send(new AddDatabaseShardOperation(store.Database, shardNumber: 4));
                Assert.Equal(4, res.ShardNumber);
                Assert.Equal(2, res.ShardTopology.ReplicationFactor);
                Assert.Equal(2, res.ShardTopology.AllNodes.Count());
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(res.RaftCommandIndex);

                await AssertWaitForValueAsync(async () =>
                {
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    return record.Sharding.Shards.Count;
                }, 3);

                //wait for the nodes to be promoted within the new shard topology
                await AssertWaitForValueAsync(async () =>
                {
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    record.Sharding.Shards.TryGetValue(4, out shardTopology);
                    return shardTopology?.Members?.Count;
                }, 2);

                var nodesContainingNewShard = shardTopology.Members;

                foreach (var node in nodesContainingNewShard)
                {
                    var serverWithNewShard = Servers.Single(x => x.ServerStore.NodeTag == node);
                    Assert.True(serverWithNewShard.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(ShardHelper.ToShardName(store.Database, 4), out _));
                }
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task CreateWholeShardForDatabase_NonSpecificParameters()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(leader, shards: 2, shardReplicationFactor: 2, orchestratorReplicationFactor: 2);

            using (var store = GetDocumentStore(options))
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var shardTopology = record.Sharding.Shards[0];
                Assert.Equal(2, shardTopology.Members.Count);
                Assert.Equal(0, shardTopology.Promotables.Count);
                Assert.Equal(2, shardTopology.ReplicationFactor);

                //create new shard
                var res = store.Maintenance.Server.Send(new AddDatabaseShardOperation(store.Database));
                var shardNumber = res.ShardNumber;
                Assert.Equal(2, shardNumber);
                Assert.Equal(2, res.ShardTopology.ReplicationFactor);
                Assert.Equal(2, res.ShardTopology.AllNodes.Count());
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(res.RaftCommandIndex);

                await AssertWaitForValueAsync(async () =>
                {
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    return record.Sharding.Shards.Count;
                }, 3);

                await AssertWaitForValueAsync(async () =>
                {
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    record.Sharding.Shards.TryGetValue(shardNumber, out shardTopology);
                    return shardTopology?.Members?.Count;
                }, 2);

                var nodesContainingNewShard = shardTopology.Members;

                foreach (var node in nodesContainingNewShard)
                {
                    var serverWithNewShard = Servers.Single(x => x.ServerStore.NodeTag == node);
                    Assert.True(serverWithNewShard.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(ShardHelper.ToShardName(store.Database, shardNumber), out _));
                }
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task CreateWholeShardForDatabase_OnSpecificNodes()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(leader, shards: 2, shardReplicationFactor: 2, orchestratorReplicationFactor: 2);

            using (var store = GetDocumentStore(options))
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var shardTopology = record.Sharding.Shards[0];
                Assert.Equal(2, shardTopology.Members.Count);
                Assert.Equal(0, shardTopology.Promotables.Count);
                Assert.Equal(2, shardTopology.ReplicationFactor);

                //create new shard
                var res = store.Maintenance.Server.Send(new AddDatabaseShardOperation(store.Database, nodes: new[] { "A", "C" }));
                var shardNumber = res.ShardNumber;
                Assert.Equal(2, shardNumber);
                Assert.Equal(2, res.ShardTopology.ReplicationFactor);
                Assert.Equal(2, res.ShardTopology.AllNodes.Count());
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(res.RaftCommandIndex);

                await AssertWaitForValueAsync(async () =>
                {
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    return record.Sharding.Shards.Count;
                }, 3);

                await AssertWaitForValueAsync(async () =>
                {
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    record.Sharding.Shards.TryGetValue(shardNumber, out shardTopology);
                    return shardTopology?.Members?.Count;
                }, 2);

                Assert.Contains("A", shardTopology.Members);
                Assert.Contains("C", shardTopology.Members);

                var nodesContainingNewShard = shardTopology.Members;

                foreach (var node in nodesContainingNewShard)
                {
                    var serverWithNewShard = Servers.Single(x => x.ServerStore.NodeTag == node);
                    Assert.True(serverWithNewShard.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(ShardHelper.ToShardName(store.Database, shardNumber), out _));
                }
            }
        }
    }
}
