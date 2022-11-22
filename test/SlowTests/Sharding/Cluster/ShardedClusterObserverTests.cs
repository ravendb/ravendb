using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using SlowTests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Cluster
{
    public class ShardedClusterObserverTests : ClusterTestBase
    {
        public ShardedClusterObserverTests(ITestOutputHelper output) : base(output)
        {
        }
        
        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task CanMoveToRehabAndBackToMember()
        {
            var cluster = await CreateRaftCluster(3, leaderIndex: 0, shouldRunInMemory: false);
            var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 3, orchestratorReplicationFactor: 3, dynamicNodeDistribution: false);
            
            options.Server = cluster.Leader;
            using (var store = GetDocumentStore(options))
            {
                var result = await DisposeServerAndWaitForFinishOfDisposalAsync(cluster.Nodes[1]);

                await AssertWaitForValueAsync(async () =>
                {
                    var shards = await ShardingCluster.GetShards(store);
                    return shards.Sum(s => s.Value.Rehabs.Count);
                }, 3);

                await AssertWaitForValueAsync(async () =>
                {
                    var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    var top = record.Sharding.Orchestrator.Topology;
                    return top.Members.Count;
                }, 2);

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var top = record.Sharding.Orchestrator.Topology;
                Assert.Equal(2, top.Members.Count);
                Assert.Equal(1, top.Rehabs.Count);
                Assert.Equal(result.NodeTag, top.Rehabs.First());
                Assert.Equal(1, top.DemotionReasons.Count);
                Assert.Equal(1, top.PromotablesStatus.Count);

                var settings = new Dictionary<string, string>
                {
                    {RavenConfiguration.GetKey(x => x.Core.ServerUrls), result.Url}
                };

                //revive the node
                var revived = GetNewServer(new ServerCreationOptions
                {
                    RunInMemory = false,
                    DeletePrevious = false,
                    DataDirectory = result.DataDirectory,
                    CustomSettings = settings,
                    NodeTag = result.NodeTag
                });
                
                await AssertWaitForValueAsync(async () =>
                {
                    var shards = await ShardingCluster.GetShards(store);
                    return shards?.Sum(s => s.Value.Members.Count);
                }, 9);
                
                await AssertWaitForValueAsync(async () =>
                {
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    top = record.Sharding.Orchestrator.Topology;
                    return top.Members.Count;
                }, 3);

                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                top = record.Sharding.Orchestrator.Topology;
                Assert.Equal(3, top.Members.Count);
                Assert.Equal(0, top.Rehabs.Count);
                Assert.Equal(0, top.DemotionReasons.Count);
                Assert.Equal(0, top.PromotablesStatus.Count);
            }
        }
        
        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task AddNodeToOrchestratorTopologyAndWaitForPromote()
        {
            var (nodes, leader) = await CreateRaftCluster(3);

            var options = Sharding.GetOptionsForCluster(leader, shards: 2, shardReplicationFactor: 1, orchestratorReplicationFactor: 2);

            using (var store = GetDocumentStore(options))
            {
                var record = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)));
                var dbTopology = record.Sharding.Orchestrator.Topology;
                Assert.Equal(2, dbTopology.Members.Count);
                Assert.Equal(0, dbTopology.Promotables.Count);

                var allNodes = new[] {"A", "B", "C"};
                
                var nodeInOrchestratorTopology = allNodes.First(x => record.Sharding.Orchestrator.Topology.Members.Contains(x));

                //remove the node from orchestrator topology
                store.Maintenance.Server.Send(new RemoveNodeFromOrchestratorTopologyOperation(store.Database, nodeInOrchestratorTopology));
                record = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)));
                dbTopology = record.Sharding.Orchestrator.Topology;
                Assert.Equal(1, dbTopology.Members.Count);
                Assert.Equal(0, dbTopology.Promotables.Count);
                Assert.Equal(0, dbTopology.Rehabs.Count);

                await AssertWaitForValueAsync(() =>
                {
                    var shardedDatabaseContext = Servers.First(x => x.ServerStore.NodeTag == nodeInOrchestratorTopology);
                    return Task.FromResult(shardedDatabaseContext.ServerStore.DatabasesLandlord.ShardedDatabasesCache.TryGetValue(store.Database, out _));
                }, false);
                
                //add node to orchestrator topology
                store.Maintenance.Server.Send(new AddNodeToOrchestratorTopologyOperation(store.Database,
                    nodeInOrchestratorTopology));
                record = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)));
                dbTopology = record.Sharding.Orchestrator.Topology;
                Assert.Equal(1, dbTopology.Members.Count);
                Assert.Equal(1, dbTopology.Promotables.Count);
                Assert.Equal(nodeInOrchestratorTopology, dbTopology.Promotables[0]);

                //wait for cluster observer to promote it to orchestrator
                await AssertWaitForValueAsync(async () =>
                {
                    record = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)));
                    dbTopology = record.Sharding.Orchestrator.Topology;
                    return dbTopology.Members.Count == 2 && dbTopology.Promotables.Count == 0;
                }, true);

                await AssertWaitForValueAsync(() =>
                {
                    var shardedDatabaseContext = Servers.First(x => x.ServerStore.NodeTag == nodeInOrchestratorTopology);
                    return Task.FromResult(shardedDatabaseContext.ServerStore.DatabasesLandlord.ShardedDatabasesCache.TryGetValue(store.Database, out _));
                }, true);
            }
        }
        
        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task DynamicNodeDistributionForOrchestrator()
        {
            DefaultClusterSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "5",
                [RavenConfiguration.GetKey(x => x.Cluster.AddReplicaTimeout)] = "5"
            };

            var cluster = await CreateRaftCluster(3);
            var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 2, shardReplicationFactor: 1, orchestratorReplicationFactor: 2, dynamicNodeDistribution: true);
            options.Server = cluster.Leader;

            using (var store = GetDocumentStore(options))
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(2, record.Sharding.Orchestrator.Topology.Members.Count);

                var server = Servers.Single(x => record.Sharding.Orchestrator.Topology.Members.First() == x.ServerStore.NodeTag);
                var result = await DisposeServerAndWaitForFinishOfDisposalAsync(server);

                await AssertWaitForValueAsync(async () =>
                {
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    return record.Sharding.Orchestrator.Topology.Members.Count;
                }, 1);
                
                var top = record.Sharding.Orchestrator.Topology;
                Assert.Equal(2, top.ReplicationFactor);
                Assert.Equal(1, top.Members.Count);
                Assert.Equal(1, top.Rehabs.Count);
                Assert.Equal(result.NodeTag, top.Rehabs.First());
                Assert.Equal(1, top.DemotionReasons.Count);
                Assert.Equal(1, top.PromotablesStatus.Count);

                var stableNode = top.Members.First();

                //wait for dynamic node distribution to kick in and choose a different node
                await AssertWaitForValueAsync(async () =>
                {
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    return record.Sharding.Orchestrator.Topology.Members.Count;
                }, 2);

                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                top = record.Sharding.Orchestrator.Topology;
                Assert.Equal(2, top.Members.Count);
                Assert.DoesNotContain(result.NodeTag, top.Members);
                Assert.Equal(0, top.Rehabs.Count);
                Assert.Equal(2, top.ReplicationFactor);
                Assert.Equal(0, top.DemotionReasons.Count);
                Assert.Equal(0, top.PromotablesStatus.Count);

                var newChosenNode = top.Members.First(x => x != stableNode);
                await AssertWaitForValueAsync(() =>
                {
                    var shardedDatabaseContext = Servers.Single(x => x.ServerStore.NodeTag == newChosenNode);
                    return Task.FromResult(shardedDatabaseContext.ServerStore.DatabasesLandlord.ShardedDatabasesCache.TryGetValue(store.Database, out _));
                }, true);
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
                    var add = new AddDatabaseNodeOperation(database, shardNumber: i);
                    await store.Maintenance.Server.SendAsync(add);
                }

                await AssertWaitForValueAsync(async () =>
                {
                    var shards = await ShardingCluster.GetShards(store);
                    return shards.Sum(s => s.Value.Members.Count);
                }, 6);
            }
        }
    }
}
