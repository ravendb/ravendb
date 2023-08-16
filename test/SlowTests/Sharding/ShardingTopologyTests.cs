using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NuGet.Packaging;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
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

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        public async Task CanToggleDynamicNodeDistributionForShardDatabase()
        {
            var options = Sharding.GetOptionsForCluster(Server, shards: 1, shardReplicationFactor: 1, orchestratorReplicationFactor: 3, dynamicNodeDistribution: true);
            using (var store = GetDocumentStore(options))
            {
                var res = store.Maintenance.Server.Send(new AddDatabaseShardOperation(store.Database, nodes: new[] {Server.ServerStore.NodeTag},
                    dynamicNodeDistribution: false));
                var newShard = res.ShardNumber;

                var record = GetDatabaseRecord(store);
                Assert.Equal(false, record.Sharding.Shards[res.ShardNumber].DynamicNodesDistribution);

                await store.Maintenance.Server.SendAsync(new SetDatabaseDynamicDistributionOperation(ShardHelper.ToShardName(store.Database, newShard),
                    allowDynamicDistribution: true));

                await AssertWaitForValueAsync(async () =>
                {
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    return record.Sharding.Shards[newShard].DynamicNodesDistribution;
                }, true);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        public Task DynamicNodeDistributionDefaultsToOtherShardsSetting()
        {
            //create db with one shard that has dynamic node distribution enabled
            var options = Sharding.GetOptionsForCluster(Server, shards: 1, shardReplicationFactor: 1, orchestratorReplicationFactor: 3, dynamicNodeDistribution: true);
            using (var store = GetDocumentStore(options))
            {
                //do not provide dynamic node distribution for new shard creation
                var res = store.Maintenance.Server.Send(new AddDatabaseShardOperation(store.Database, nodes: new[] { Server.ServerStore.NodeTag }));
                
                //new shard will have it enabled
                var record = GetDatabaseRecord(store);
                Assert.Equal(true, record.Sharding.Shards[res.ShardNumber].DynamicNodesDistribution);
            }

            return Task.CompletedTask;
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

                var shardToDelete = await Sharding.GetShardNumberForAsync(store, "users/1");

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
                Assert.Contains(
                    $"Database {ShardHelper.ToShardName(store.Database, 2)} cannot be deleted because it is the last copy of shard {2} and it contains data that has not been migrated",
                    error.Message);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task NewShardShouldBeAddedToANodeThatIsMostFree()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(leader, shards: 2, shardReplicationFactor: 1, orchestratorReplicationFactor: 2);

            using (var store = GetDocumentStore(options))
            {
                //add a new shard and check it is added to the free node
                var res = store.Maintenance.Server.Send(new AddDatabaseShardOperation(store.Database, replicationFactor: 1));
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(res.RaftCommandIndex);

                var sharding = await Sharding.GetShardingConfigurationAsync(store);
                var nodeToInstanceCount = new Dictionary<string, int>();

                foreach (var (shardNumber, topology) in sharding.Shards)
                {
                    foreach (var node in topology.Members)
                    {
                        nodeToInstanceCount[node] = nodeToInstanceCount.ContainsKey(node) ? nodeToInstanceCount[node] + 1 : 1;
                    }
                }

                Assert.Equal(3, nodeToInstanceCount.Count);
                foreach (var (node, count) in nodeToInstanceCount)
                {
                    Assert.Equal(1, count);
                }
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task EnsureCantAddShardReplicaWhenAllClusterNodesAreTaken()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(leader, shards: 1, shardReplicationFactor: 3, orchestratorReplicationFactor: 2);

            using (var store = GetDocumentStore(options))
            {
                var sharding = await Sharding.GetShardingConfigurationAsync(store);
                Assert.Equal(3, sharding.Shards.First().Value.Count);

                //try to add a new shard with a list of the same node twice
                var error = Assert.ThrowsAny<RavenException>(() =>
                {
                    store.Maintenance.Server.Send(new AddDatabaseNodeOperation(store.Database, shardNumber: sharding.Shards.Keys.First()));
                });
                Assert.Contains("already exists on all the nodes of the cluster", error.Message);
            }
        }
        
        [RavenTheory(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public void EnsureTopologyCantContainDuplicates(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                //try to add a new shard with a list of the same node twice
                var error = Assert.ThrowsAny<RavenException>(() =>
                {
                    store.Maintenance.Server.Send(new AddDatabaseShardOperation(store.Database, nodes: new[] { "A", "A" }));
                });
                Assert.Contains("The provided list of nodes contains duplicates", error.Message);
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

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public void ValidateShardCantHaveMultipleInstancesOnSameNode()
        {
            var error = Assert.ThrowsAny<RavenException>(() =>
            {
                using (var store = GetDocumentStore(new Options()
                       {
                           ModifyDatabaseRecord = r =>
                           {
                               r.Sharding = new ShardingConfiguration();
                               r.Sharding.Shards = new Dictionary<int, DatabaseTopology>()
                               {
                                   {0, new DatabaseTopology() {Members = new List<string>() {"A", "A"}}},
                                   {1, new DatabaseTopology()},
                                   {2, new DatabaseTopology()}
                               };
                           }
                       }))
                {
                    
                }
            });
            Assert.Contains("cannot have multiple replicas reside on the same node", error.Message);
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
                var chosenShard = record.Sharding.Shards.Keys.First();
                var shardTopology = record.Sharding.Shards[chosenShard];
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
                Assert.False(serverWithNewShard.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(ShardHelper.ToShardName(store.Database, chosenShard), out var _));

                leader.ServerStore.Observer.Suspended = true;

                //duplicate shard to node
                var res = store.Maintenance.Server.Send(new AddDatabaseNodeOperation(store.Database, shardNumber: chosenShard, node: nodeNotContainingShards));
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(res.RaftCommandIndex);

                await AssertWaitForValueAsync(async () =>
                {
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    shardTopology = record.Sharding.Shards[chosenShard];
                    return shardTopology.Promotables.Count;
                }, 1);

                Assert.Equal(1, shardTopology.Members.Count);
                Assert.Equal(1, shardTopology.Promotables.Count);
                Assert.Equal(nodeNotContainingShards, shardTopology.Promotables[0]);

                //promote immediately
                await store.Maintenance.Server.SendAsync(new PromoteDatabaseNodeOperation(store.Database, chosenShard, nodeNotContainingShards));

                await AssertWaitForValueAsync(async () =>
                {
                    var t = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)));
                    shardTopology = t.Sharding.Shards[chosenShard];
                    return shardTopology.Members.Count;
                }, 2);

                Assert.Equal(2, shardTopology.Members.Count);
                Assert.Equal(0, shardTopology.Promotables.Count);
                Assert.Equal(2, shardTopology.ReplicationFactor);

                serverWithNewShard = Servers.Single(x => x.ServerStore.NodeTag == nodeNotContainingShards);
                Assert.True(serverWithNewShard.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(ShardHelper.ToShardName(store.Database, chosenShard), out var _));
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task PreventRemovingLastOrchestrator()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(leader, shards: 2, shardReplicationFactor: 1, orchestratorReplicationFactor: 2);

            using (var store = GetDocumentStore(options))
            {
                var record = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)));
                var dbTopology = record.Sharding.Orchestrator.Topology;
                Assert.Equal(2, dbTopology.Members.Count);
                Assert.Equal(0, dbTopology.Promotables.Count);

                var orchestratorNodes = dbTopology.Members;

                //remove the node from orchestrator topology
                store.Maintenance.Server.Send(new RemoveNodeFromOrchestratorTopologyOperation(store.Database, orchestratorNodes[0]));

                await AssertWaitForValueAsync(async () =>
                {
                    record = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)));
                    dbTopology = record.Sharding.Orchestrator.Topology;
                    return dbTopology.Members.Count;
                }, 1);

                //try remove last orchestrator
                var error = Assert.ThrowsAny<RavenException>(() =>
                {
                    store.Maintenance.Server.Send(new RemoveNodeFromOrchestratorTopologyOperation(store.Database, orchestratorNodes[1]));
                });
                Assert.Contains("orchestrator topology because it is the only one in the topology", error.Message);

                record = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)));
                dbTopology = record.Sharding.Orchestrator.Topology;
                Assert.Equal(1, dbTopology.Members.Count);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task RavenDB_19998_EnsureReplicationFactorOfOrchestratorUpdates()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);

            var options = Sharding.GetOptionsForCluster(leader, shards: 2, shardReplicationFactor: 1, orchestratorReplicationFactor: 2);

            using (var store = GetDocumentStore(options))
            {
                var record = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)));
                var dbTopology = record.Sharding.Orchestrator.Topology;
                Assert.Equal(2, record.Sharding.Orchestrator.Topology.ReplicationFactor);
                Assert.Equal(2, dbTopology.Members.Count);
                Assert.Equal(0, dbTopology.Promotables.Count);

                var allNodes = new[] { "A", "B", "C" };

                var nodeAmount = record.Sharding.Orchestrator.Topology.Members.Count;
                var nodeInOrchestratorTopology = allNodes.First(x => record.Sharding.Orchestrator.Topology.Members.Contains(x));

                //remove the node from orchestrator topology
                var modifyResult = store.Maintenance.Server.Send(new RemoveNodeFromOrchestratorTopologyOperation(store.Database, nodeInOrchestratorTopology));
                record = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)));
                dbTopology = record.Sharding.Orchestrator.Topology;
                Assert.Equal(nodeAmount - 1, dbTopology.Members.Count);
                Assert.Equal(1, record.Sharding.Orchestrator.Topology.ReplicationFactor);
                Assert.Equal(0, dbTopology.Promotables.Count);
                Assert.Equal(0, dbTopology.Rehabs.Count);

                Assert.Equal(dbTopology.Members.Count, modifyResult.OrchestratorTopology.Members.Count);
                Assert.Equal(dbTopology.Rehabs.Count, modifyResult.OrchestratorTopology.Rehabs.Count);

                //add node to orchestrator topology
                modifyResult = store.Maintenance.Server.Send(new AddNodeToOrchestratorTopologyOperation(store.Database, nodeInOrchestratorTopology));
                record = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)));
                dbTopology = record.Sharding.Orchestrator.Topology;
                Assert.Equal(2, record.Sharding.Orchestrator.Topology.ReplicationFactor);
                Assert.Equal(nodeAmount - 1, dbTopology.Members.Count);
                Assert.Equal(1, dbTopology.Promotables.Count);
                Assert.Equal(nodeInOrchestratorTopology, dbTopology.Promotables[0]);

                Assert.Equal(dbTopology.Members.Count, modifyResult.OrchestratorTopology.Members.Count);
                Assert.Equal(dbTopology.Rehabs.Count, modifyResult.OrchestratorTopology.Rehabs.Count);
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
                var chosenShard = record.Sharding.Shards.Keys.First();

                var shardTopology = record.Sharding.Shards[chosenShard];
                Assert.Equal(2, shardTopology.Members.Count);
                Assert.Equal(0, shardTopology.Promotables.Count);
                Assert.Equal(2, shardTopology.ReplicationFactor);

                var nodeContainingShard = shardTopology.Members.First();

                var serverWithNewShard = Servers.Single(x => x.ServerStore.NodeTag == nodeContainingShard);
                Assert.True(serverWithNewShard.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(ShardHelper.ToShardName(store.Database, chosenShard), out var _));

                //remove shard from node
                var res = store.Maintenance.Server.Send(new DeleteDatabasesOperation(store.Database, shardNumber: chosenShard, hardDelete: true, fromNode: nodeContainingShard));
                Assert.Equal(1, res.PendingDeletes.Length);
                Assert.True(res.PendingDeletes.Contains(nodeContainingShard));
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(res.RaftCommandIndex);

                await AssertWaitForValueAsync(async () =>
                {
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    shardTopology = record.Sharding.Shards[chosenShard];
                    return shardTopology.Members.Count;
                }, 1);

                Assert.DoesNotContain(nodeContainingShard, shardTopology.Members);

                serverWithNewShard = Servers.Single(x => x.ServerStore.NodeTag == nodeContainingShard);
                Assert.False(serverWithNewShard.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(ShardHelper.ToShardName(store.Database, chosenShard), out var _));
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
                var chosenShard = record.Sharding.Shards.Keys.First();
                var shardTopology = record.Sharding.Shards[chosenShard];
                Assert.Equal(1, shardTopology.Members.Count);
                Assert.Equal(0, shardTopology.Promotables.Count);
                Assert.Equal(1, shardTopology.ReplicationFactor);

                var nodeContainingShard = shardTopology.Members.First();

                //remove shard from node
                var error = Assert.ThrowsAny<RavenException>(() =>
                    store.Maintenance.Server.Send(new DeleteDatabasesOperation(store.Database, shardNumber: chosenShard, hardDelete: true, fromNode: nodeContainingShard)));

                Assert.Contains(
                    $"Database {ShardHelper.ToShardName(store.Database, chosenShard)} cannot be deleted because it is the last copy of shard {chosenShard} and it contains data that has not been migrated",
                    error.Message);

                //topology should not change
                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                shardTopology = record.Sharding.Shards[chosenShard];
                Assert.Equal(1, shardTopology.Members.Count);
                Assert.Equal(0, shardTopology.Promotables.Count);
                Assert.Equal(1, shardTopology.ReplicationFactor);

                var serverWithShard = Servers.Single(x => x.ServerStore.NodeTag == nodeContainingShard);
                Assert.True(serverWithShard.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(ShardHelper.ToShardName(store.Database, chosenShard), out var _));
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

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task RemoveNonRelatedClusterNode()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true, leaderIndex: 0);

            var options = new Options
            {
                DatabaseMode = RavenDatabaseMode.Sharded,
                ModifyDatabaseRecord = r =>
                {
                    r.Sharding = new ShardingConfiguration
                    {
                        Shards = new Dictionary<int, DatabaseTopology>(2),
                        Orchestrator = new OrchestratorConfiguration
                        {
                            Topology = new OrchestratorTopology
                            {
                                Members = new List<string>{"A", "B"},
                                ReplicationFactor = 2
                            }
                        }
                    };

                    r.Sharding.Shards[0] = new DatabaseTopology
                    {
                        Members = new List<string>{"A"},
                        ReplicationFactor = 1,
                    };
                    r.Sharding.Shards[1] = new DatabaseTopology
                    {
                        Members = new List<string>{"B"},
                        ReplicationFactor = 1,
                    };
                },
                ReplicationFactor = 1, // this ensures not to use the same path for the replicas
                Server = leader
            };

            using (var store = GetDocumentStore(options))
            {
                await leader.ServerStore.Engine.RemoveFromClusterAsync("C");
                await DisposeAndRemoveServer(Servers.Single(s => s.ServerStore.NodeTag == "C"));
                
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.NotNull(record);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task RemoveClusterNodeWithAllShardsOnIt()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true, leaderIndex: 0);
            var options = new Options
            {
                DatabaseMode = RavenDatabaseMode.Sharded,
                ModifyDatabaseRecord = r =>
                {
                    r.Sharding = new ShardingConfiguration
                    {
                        Shards = new Dictionary<int, DatabaseTopology>(),
                        Orchestrator = new OrchestratorConfiguration
                        {
                            Topology = new OrchestratorTopology
                            {
                                Members = new List<string>{"C"},
                                ReplicationFactor = 1
                            }
                        }
                    };

                    r.Sharding.Shards[0] = new DatabaseTopology
                    {
                        Members = new List<string>{"C"},
                        ReplicationFactor = 1,
                    };
                    r.Sharding.Shards[1] = new DatabaseTopology
                    {
                        Members = new List<string>{"C"},
                        ReplicationFactor = 1,
                    };
                },
                ReplicationFactor = 1, // this ensures not to use the same path for the replicas
                Server = leader
            };

            using (var store = GetDocumentStore(options))
            {
                await leader.ServerStore.Engine.RemoveFromClusterAsync("C");
                await DisposeAndRemoveServer(Servers.Single(s => s.ServerStore.NodeTag == "C"));
                
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Null(record);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task RemoveClusterNodeWithSomeShardsOnIt()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true, leaderIndex: 0);
            var options = new Options
            {
                DatabaseMode = RavenDatabaseMode.Sharded,
                ModifyDatabaseRecord = r =>
                {
                    r.Sharding = new ShardingConfiguration
                    {
                        Shards = new Dictionary<int, DatabaseTopology>(),
                        Orchestrator = new OrchestratorConfiguration
                        {
                            Topology = new OrchestratorTopology
                            {
                                Members = new List<string>{"A", "B", "C"},
                                ReplicationFactor = 3
                            }
                        }
                    };

                    r.Sharding.Shards[0] = new DatabaseTopology
                    {
                        Members = new List<string>{"A","B"},
                        ReplicationFactor = 2,
                    };
                    r.Sharding.Shards[1] = new DatabaseTopology
                    {
                        Members = new List<string>{"B", "C"},
                        ReplicationFactor = 2
                    };
                    r.Sharding.Shards[2] = new DatabaseTopology
                    {
                        Members = new List<string>{"A", "C"},
                        ReplicationFactor = 2
                    };
                },
                ReplicationFactor = 2, // this ensures not to use the same path for the replicas
                Server = leader
            };

            using (var store = GetDocumentStore(options))
            {
                await leader.ServerStore.Engine.RemoveFromClusterAsync("C");
                await DisposeAndRemoveServer(Servers.Single(s => s.ServerStore.NodeTag == "C"));
                
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.NotNull(record);

                Assert.Equal(2, record.Sharding.Orchestrator.Topology.Count);
                Assert.Contains(record.Sharding.Orchestrator.Topology.AllNodes, n => n == "A" || n == "B");

                Assert.Equal(2, record.Sharding.Shards[0].Count);
                Assert.Contains(record.Sharding.Shards[0].AllNodes, n => n == "A" || n == "B");

                Assert.Equal(1, record.Sharding.Shards[1].Count);
                Assert.Contains(record.Sharding.Shards[1].AllNodes, n => n == "B");

                Assert.Equal(1, record.Sharding.Shards[2].Count);
                Assert.Contains(record.Sharding.Shards[2].AllNodes, n => n == "A");
            }
        }
    }
}
