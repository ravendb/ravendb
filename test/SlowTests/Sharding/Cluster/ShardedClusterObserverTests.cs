using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.ServerWide.Sharding;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using SlowTests.Core.Utils.Entities;
using Sparrow.Json;
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
            var cluster = await CreateRaftCluster(3, leaderIndex: 0, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 2, shardReplicationFactor: 1, orchestratorReplicationFactor: 2, dynamicNodeDistribution: true);
            
            using (var store = GetDocumentStore(options))
            {
                var record = await store.Maintenance.Server.ForNode(cluster.Leader.ServerStore.NodeTag).SendAsync(new GetDatabaseRecordOperation(store.Database));
                var originalOrchestrators = record.Sharding.Orchestrator.Topology.Members;
                Assert.True(2 == originalOrchestrators.Count, $"Topology: {record.Sharding.Orchestrator.Topology}");

                var serverToDispose = cluster.Nodes.First(x =>
                    record.Sharding.Orchestrator.Topology.Members.Contains(x.ServerStore.NodeTag) && cluster.Leader.ServerStore.NodeTag != x.ServerStore.NodeTag);
                var disposedServer = await DisposeServerAndWaitForFinishOfDisposalAsync(serverToDispose);

                var remainingOriginalOrchestrator = originalOrchestrators.FirstOrDefault(x => x != serverToDispose.ServerStore.NodeTag);
                Assert.True(remainingOriginalOrchestrator != null, "remainingOriginalOrchestrator is null");

                //wait for dynamic node distribution to kick in and choose a different node
                await WaitForValueAsync(async () =>
                {
                    record = await store.Maintenance.Server.ForNode(remainingOriginalOrchestrator).SendAsync(new GetDatabaseRecordOperation(store.Database));
                    return record.Sharding.Orchestrator.Topology.Members.Count == 2 &&
                           record.Sharding.Orchestrator.Topology.Members.Contains(disposedServer.NodeTag) == false;
                }, true);

                var top = record.Sharding.Orchestrator.Topology;
                Assert.True(2 == top.Members.Count, $"topology: {top}");
                Assert.DoesNotContain(disposedServer.NodeTag, top.Members);
                Assert.Contains(remainingOriginalOrchestrator, top.Members);
                Assert.True(0 == top.Rehabs.Count, $"topology has rehab nodes: {top}");
                Assert.True(2 == top.ReplicationFactor, $"ReplicationFactor incorrect. topology: {top}");
                Assert.True(0 == top.DemotionReasons.Count, $"DemotionReasons is not empty. Topology: {top}");
                Assert.True(0 == top.PromotablesStatus.Count, $"PromotablesStatus is not empty. Topology: {top}");

                var newChosenNode = top.Members.First(x => x != remainingOriginalOrchestrator);
                await AssertWaitForValueAsync(() =>
                {
                    var shardedDatabaseContext = cluster.Nodes.Single(x => x.ServerStore.NodeTag == newChosenNode);
                    return Task.FromResult(shardedDatabaseContext.ServerStore.DatabasesLandlord.ShardedDatabasesCache.TryGetValue(store.Database, out _));
                }, true);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task ClusterObserverWillSkipCommandIfChangingTheSameDatabaseRecordTwiceInOneIteration()
        {
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Cluster.SupervisorSamplePeriod)] = "5000";

            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 1, orchestratorReplicationFactor: 3, dynamicNodeDistribution: true);
            options.Server = cluster.Leader;
            
            using (var store = GetDocumentStore(options))
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(3, record.Sharding.Orchestrator.Topology.Members.Count);
                
                cluster.Leader.ServerStore.Observer.Suspended = true;

                var nodeToBringDown = record.Sharding.Orchestrator.Topology.Members.First(x => x != cluster.Leader.ServerStore.NodeTag);
                var shardBroughtDown = record.Sharding.Shards.Single(shardTop => shardTop.Value.AllNodes.Contains(nodeToBringDown)).Key;
                var server = Servers.Single(x => nodeToBringDown == x.ServerStore.NodeTag);
                var result = await DisposeServerAndWaitForFinishOfDisposalAsync(server);

                cluster.Leader.ServerStore.Observer.Suspended = false;
                
                await AssertWaitForValueAsync(async () =>
                {
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    if (record.Sharding.Orchestrator.Topology.Rehabs.Count == 1)
                    {
                        cluster.Leader.ServerStore.Observer.Suspended = true;
                        //observerLastIteration = cluster.Leader.ServerStore.Observer.
                    }

                    return record.Sharding.Orchestrator.Topology.Rehabs.Count;
                }, 1);

                //decisions log commands will only contain orchestrator change but not the shard topology changes
                var shardRehabCommandFired = false;
                var orchestratorRehabCommandFired = false;
                var (logs, iteration) = cluster.Leader.ServerStore.Observer.ReadDecisionsForDatabase();
                foreach (var log in logs)
                {
                    if (log.Message.Contains("moved to rehab") && log.Database == ShardHelper.ToShardName(store.Database, shardBroughtDown))
                        shardRehabCommandFired = true;
                    if (log.Message.Contains("moved to rehab") && log.Database == store.Database)
                        orchestratorRehabCommandFired = true;
                }

                Assert.False(shardRehabCommandFired);
                Assert.True(orchestratorRehabCommandFired);

                var concurrencyException = false;

                using (cluster.Leader.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (context.OpenReadTransaction())
                {
                    string exception;
                    foreach (var entry in cluster.Leader.ServerStore.Engine.LogHistory.GetHistoryLogs(context))
                    {
                        var type = entry[nameof(RachisLogHistory.LogHistoryColumn.Type)].ToString();
                        if (type == "UpdateTopologyCommand")
                        {
                            exception = (entry[nameof(RachisLogHistory.LogHistoryColumn.ExceptionMessage)]?.ToString());
                            if (exception?.Contains("Concurrency violation") == true)
                                concurrencyException = true;
                        }
                    }
                }
                Assert.False(concurrencyException);

                await AssertWaitForValueAsync(() =>
                {
                    var dbRecord = GetDatabaseRecord(store);
                    return Task.FromResult(dbRecord.Sharding.Orchestrator.Topology.Rehabs.Count);
                }, 1);

                await AssertWaitForValueAsync(() =>
                {
                    var dbRecord = GetDatabaseRecord(store);
                    return Task.FromResult(dbRecord.Sharding.Shards[shardBroughtDown].Rehabs.Count);
                }, 0);
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
                var config = await Sharding.GetShardingConfigurationAsync(store);
                foreach (var shardNumber in config.Shards.Keys)
                {
                    var add = new AddDatabaseNodeOperation(database, shardNumber: shardNumber);
                    await store.Maintenance.Server.SendAsync(add);
                }
                
                await AssertWaitForValueAsync(async () =>
                {
                    var shards = await ShardingCluster.GetShards(store);
                    return shards.Sum(s => s.Value.Members.Count);
                }, 6);
            }
        }
        
        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task AddingNewNodeToShardShouldUpdateShardRequestExecutorTopology_RavenDB_20702()
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true, leaderIndex: 0);

            var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 1, orchestratorReplicationFactor: 1);
            using (var store = GetDocumentStore(options))
            {
                await RevisionsHelper.SetupRevisionsAsync(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                DatabaseRecord record = GetDatabaseRecord(store);
                var orchestratorNodeTag = record.Sharding.Orchestrator.Topology.Members.Single();

                var shardNumber = await Sharding.GetShardNumberForAsync(store, "users/1");
                var originalNode = GetDatabaseRecord(store).Sharding.Shards[shardNumber].Members.Single();
                var newNode = (new string[] { "A", "B", "C" }).First(x => x != originalNode);

                var topologyNodes = await GetExecutorTopologyNodeForShard(orchestratorNodeTag, shardNumber, store.Database, cluster.Nodes);
                Assert.Equal(1, topologyNodes.Count);
                Assert.Equal(originalNode, topologyNodes[0]);

                //add new node to the shard
                await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database, shardNumber: shardNumber, node: newNode));

                await WaitAndAssertForValueAsync(() =>
                {
                    record = GetDatabaseRecord(store);
                    return record.Sharding.Shards[shardNumber].Members.Count;
                }, 2);
                
                //check the shard executor's topology changed in the orchestrator node
                await WaitAndAssertForValueAsync(async () =>
                {
                    topologyNodes = await GetExecutorTopologyNodeForShard(orchestratorNodeTag, shardNumber, store.Database, cluster.Nodes);
                    return topologyNodes.Contains(newNode);
                }, true, 30_000);
                
                Assert.Equal(2, topologyNodes.Count);
                Assert.True(topologyNodes.Contains(originalNode));
            }
        }

        private async Task<List<string>> GetExecutorTopologyNodeForShard(string orchestrator, int shard, string database, List<RavenServer> nodes)
        {
            var orchestratorNode = nodes.Single(x => x.ServerStore.NodeTag == orchestrator);
            orchestratorNode.ServerStore.DatabasesLandlord.ShardedDatabasesCache.TryGetValue(database, out var orchTask);
            var orchestratorDatabaseContext = await orchTask;
            return orchestratorDatabaseContext.ShardExecutor.GetRequestExecutorAt(shard).TopologyNodes.Select(x => x.ClusterTag).ToList();
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task RavenDB_19936()
        {
            var database = GetDatabaseName();
            var settings = new Dictionary<string, string>
            {
                { RavenConfiguration.GetKey(x => x.Indexing.CleanupInterval), "0" },
            };
            var cluster = await CreateRaftCluster(3, watcherCluster: true, leaderIndex: 0, customSettings: settings);
            await ShardingCluster.CreateShardedDatabaseInCluster(database, replicationFactor: 1, cluster, shards: 3);

            using (var store = GetDocumentStore(new Options
                   {
                       Server = cluster.Leader,
                       CreateDatabase = false,
                       ModifyDatabaseName = _ => database,
                   }))
            {
                //create auto index
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User() {Name = "Jane"});
                    await session.SaveChangesAsync();

                    await session.Advanced
                        .AsyncRawQuery<dynamic>("from \"Users\" where search(\"Title\",$p0) select Name limit 128")
                        .AddParameter("p0", "Jane")
                        .Statistics(out var stat).ToListAsync();
                }
                
                store.Maintenance.Send(new PutDatabaseSettingsOperation(store.Database, new Dictionary<string, string>
                {
                    { RavenConfiguration.GetKey(x => x.Indexing.TimeToWaitBeforeMarkingAutoIndexAsIdle), "0" },
                }));

                var exception = "";
                //check history logs for thrown error
                var errored = await WaitForValueAsync(() =>
                {
                    using (cluster.Leader.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        foreach (var entry in cluster.Leader.ServerStore.Engine.LogHistory.GetHistoryLogs(context))
                        {
                            var type = entry[nameof(RachisLogHistory.LogHistoryColumn.Type)].ToString();
                            if (type == "SetIndexStateCommand")
                            {
                                exception = (entry[nameof(RachisLogHistory.LogHistoryColumn.ExceptionMessage)]?.ToString());
                                return exception?.Contains("Could not execute update command of type 'SetIndexStateCommand'") == true;
                            }
                        }
                    }

                    exception = "SetIndexStateCommand wasn't found in logs";
                    return true;
                }, false);
                
                Assert.False(errored, exception);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task RavenDB_19936_EnsureAutoIndexIdleOnlyWhenIdleOnAllShards()
        {
            var database = GetDatabaseName();
            var settings = new Dictionary<string, string>
            {
                { RavenConfiguration.GetKey(x => x.Indexing.CleanupInterval), "0" },
            };
            var cluster = await CreateRaftCluster(3, watcherCluster: true, leaderIndex: 0, customSettings: settings, shouldRunInMemory: true);
            await ShardingCluster.CreateShardedDatabaseInCluster(database, replicationFactor: 1, cluster, shards: 3);

            using (var store = Sharding.GetDocumentStore(new Options {Server = cluster.Leader, CreateDatabase = false, ModifyDatabaseName = _ => database,}))
            {
                //create auto index
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User() {Name = "Jane"});
                    await session.SaveChangesAsync();

                    await session.Advanced
                        .AsyncRawQuery<dynamic>("from \"Users\" where search(\"Title\",$p0) select Name limit 128")
                        .AddParameter("p0", "Jane")
                        .Statistics(out var stat).ToListAsync();
                }
                
                store.Maintenance.Send(new PutDatabaseSettingsOperation(store.Database,
                    new Dictionary<string, string> {{RavenConfiguration.GetKey(x => x.Indexing.TimeToWaitBeforeMarkingAutoIndexAsIdle), "0"},}));

                var db = await Sharding.GetAnyShardDocumentDatabaseInstanceFor(ShardHelper.ToShardName(store.Database, 0), cluster.Nodes);
                var autoIndex = db.IndexStore.GetIndexes().First();
                
                //make sure the index on all shards is idle
                var idle = await WaitForValueAsync(async () =>
                {
                    var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    return record.AutoIndexes.Count == 1 && IndexState.Idle == record.AutoIndexes.First().Value.State;
                }, true);

                Assert.True(idle);
                var sharding = await Sharding.GetShardingConfigurationAsync(store);
                var idleInMem = await WaitForValueAsync(async () =>
                {
                    var idleInMem = true;
                    foreach (var shardNumber in sharding.Shards.Keys)
                    {
                        db = await Sharding.GetAnyShardDocumentDatabaseInstanceFor(ShardHelper.ToShardName(store.Database, shardNumber), cluster.Nodes);
                        autoIndex = db.IndexStore.GetIndexes().First();
                        idleInMem = idleInMem && IndexState.Idle == autoIndex.State;
                    }

                    return idleInMem;
                }, true);
                
                Assert.True(idleInMem);

                store.Maintenance.Send(new PutDatabaseSettingsOperation(store.Database,
                    new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Indexing.TimeToWaitBeforeMarkingAutoIndexAsIdle), "30" }, }));

                //run index query only on one shard
                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, 0)))
                {
                    await session.Advanced
                        .AsyncRawQuery<dynamic>("from \"Users\" where search(\"Title\",$p0) select Name limit 128")
                        .AddParameter("p0", "Jane")
                        .Statistics(out var stat).ToListAsync();
                }

                //wait for it to stop being idle on all shards
                sharding = await Sharding.GetShardingConfigurationAsync(store);
                var normalInMem = await WaitForValueAsync(async () =>
                {
                    var normal = true;
                    foreach (var shardNumber in sharding.Shards.Keys)
                    {
                        db = await Sharding.GetAnyShardDocumentDatabaseInstanceFor(ShardHelper.ToShardName(store.Database, shardNumber), cluster.Nodes);
                        autoIndex = db.IndexStore.GetIndexes().First();
                        normal = normal && (IndexState.Normal == autoIndex.State);
                    }

                    return normal;
                }, true);

                Assert.True(normalInMem);

                var normalOnRecord = await WaitForValueAsync(async () =>
                {
                    var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    return record.AutoIndexes.Count == 1 && IndexState.Normal == record.AutoIndexes.First().Value.State;
                }, true);

                Assert.True(normalOnRecord);
            }
        }

        [Fact]
        public async Task CompareExchangeTombstonesWillBeCleanedWhenSomeShardsNeverBackedUp()
        {
            var database = GetDatabaseName();
            var settings = new Dictionary<string, string>
            {
                { RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeTombstonesCleanupInterval), "0" },
            };
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true, leaderIndex: 0, customSettings: settings);
            await ShardingCluster.CreateShardedDatabaseInCluster(database, replicationFactor: 1, (nodes, leader), shards: 3);

            var backupPath = NewDataPath(suffix: $"BackupFolder");

            using (var store = new DocumentStore()
            {
                Database = database,
                Urls = new string[] { leader.WebUrl }
            })
            {
                store.Initialize();
                var user = new User
                {
                    Name = "🤡"
                };

                //find shard number instance on leader
                var shardingConfig = await Sharding.GetShardingConfigurationAsync(store);

                var serverToShard = new Dictionary<RavenServer, int>();
                foreach (var server in nodes)
                {
                    var shard = shardingConfig.Shards.First(x => x.Value.Members.Contains(server.ServerStore.NodeTag)).Key;
                    serverToShard[server] = shard;
                }

                var shardOnLeader = serverToShard[leader];

                var shardToCX = new Dictionary<int, CompareExchangeResult<User>>();
                var shard2User = "users/1";
                Assert.Equal(2, await Sharding.GetShardNumberForAsync(store, shard2User));
                var shard0User = "users/0";
                Assert.Equal(0, await Sharding.GetShardNumberForAsync(store, shard0User));
                var shard1User = "users/6";
                Assert.Equal(1, await Sharding.GetShardNumberForAsync(store, shard1User));

                //create one 3 compare exchanges in cluster
                shardToCX[0] = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>(shard0User, user, 0));
                shardToCX[1] = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>(shard1User, user, 0));
                shardToCX[2] = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>(shard2User, user, 0));

                //put backup configuration and wait for responsible node to be chosen by observer before we suspend it
                var config = Backup.CreateBackupConfiguration(backupPath, incrementalBackupFrequency: "0 0 1 * *");
                var backupTaskId = await Sharding.Backup.UpdateConfigAsync(leader, config, store);

                //suspend observer to stall tombstone cleaning
                leader.ServerStore.Observer.Suspended = true;

                //stall the periodic backup on 2 shards
                var tcs = new TaskCompletionSource<object>();
                var server1Database = await Sharding.GetAnyShardDocumentDatabaseInstanceFor(ShardHelper.ToShardName(database, serverToShard[nodes[1]]), new List<RavenServer>() { nodes[1] });
                server1Database.PeriodicBackupRunner.ForTestingPurposesOnly().OnBackupTaskRunHoldBackupExecution = tcs;
                var server2Database = await Sharding.GetAnyShardDocumentDatabaseInstanceFor(ShardHelper.ToShardName(database, serverToShard[nodes[2]]), new List<RavenServer>() { nodes[2] });
                server2Database.PeriodicBackupRunner.ForTestingPurposesOnly().OnBackupTaskRunHoldBackupExecution = tcs;

                var timeBeforeCxDeletion = DateTime.UtcNow;

                //delete the compare exchanges
                await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>(shard0User, shardToCX[0].Index));
                await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>(shard1User, shardToCX[1].Index));
                await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>(shard2User, shardToCX[2].Index));
                await TriggerAClusterTransactionToForceCheckTombstonesAsync(store);

                await AssertCompareExchangesAsync(database, expectedCompareExchanges: 0, expectedTombstones: 3, nodes);

                //run periodic backup on leader
                await Sharding.Backup.RunBackupAsync(store.Database, backupTaskId, isFullBackup: false, new List<RavenServer>(){ leader });

                //wait for periodic backup to finish running
                var done = await WaitForValueAsync(() =>
                {
                    using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var itemName = PeriodicBackupStatus.GenerateItemName(ShardHelper.ToShardName(database, shardOnLeader), backupTaskId);
                        var status = leader.ServerStore.Cluster.Read(context, itemName);
                        if (status == null)
                            return false;
                        status.TryGet(nameof(LastRaftIndex), out BlittableJsonReaderObject lastRaftIndexBlittable);
                        lastRaftIndexBlittable.TryGet(nameof(LastRaftIndex.LastEtag), out long etag);
                        
                        return etag >= shardToCX[shardOnLeader].Index;
                    }
                }, true);
                
                Assert.True(done);

                await AssertCompareExchangesAsync(database, expectedCompareExchanges: 0, expectedTombstones: 3, nodes);

                await TriggerAClusterTransactionToForceCheckTombstonesAsync(store);
                //unsuspend and wait for the tombstone cleaner
                leader.ServerStore.Observer.Suspended = false;

                //wait for the servers to execute the cleanup command waiting in the state machine
                //(_lastTombstonesCleanupTimeInTicks only represents last time LOOKED for something to delete)
                await WaitAndAssertForGreaterThanAsync(() => Task.FromResult(leader.ServerStore.Observer._lastTombstonesCleanupTimeInTicks), timeBeforeCxDeletion.Ticks);

                //ensure no compare exchange tombstones were deleted after the tombstone cleanup
                await AssertCompareExchangesAsync(database, expectedCompareExchanges: 0, expectedTombstones: 0, nodes);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task CmpxchgTombstoneCleaner_WhenClusterTransactionMadeOnOneShard_ShouldUpdateRaftIndexOnAllShards()
        {
            const string id = "TestObjs/0";
            const string id2 = "TestObjs/2";
            
            var database = GetDatabaseName();
            var settings = new Dictionary<string, string>
            {
                { RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeTombstonesCleanupInterval), "0" },
            };

            using var store = GetDocumentStore(Sharding.GetOptionsForCluster(Server, 2, 1, 1));
            Assert.NotEqual(await Sharding.GetShardNumberForAsync(store, id), await Sharding.GetShardNumberForAsync(store, id2));
            
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new TestOjb(), id);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions{TransactionMode = TransactionMode.ClusterWide, DisableAtomicDocumentWritesInClusterWideTransaction = true}))
            {
                await session.StoreAsync(new TestOjb(), id2);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                _ = await session.LoadAsync<TestOjb>(id);
            }
        }

        [Fact]
        public async Task CompareExchangeTombstoneWillBeCleanedOnlyWhenAllShardsHaveBackedUpPreviousOnes()
        {
            var database = GetDatabaseName();
            var settings = new Dictionary<string, string>
            {
                { RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeTombstonesCleanupInterval), "0" },
            };
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true, leaderIndex: 0, customSettings: settings);
            await ShardingCluster.CreateShardedDatabaseInCluster(database, replicationFactor: 1, (nodes, leader), shards: 3);

            var backupPath = NewDataPath(suffix: $"BackupFolder");

            using (var store = new DocumentStore()
            {
                Database = database,
                Urls = new string[] { leader.WebUrl }
            }.Initialize())
            {
                var user = new User
                {
                    Name = "🤡"
                };

                //find shard number instance on leader
                var shardingConfig = await Sharding.GetShardingConfigurationAsync(store);

                var serverToShard = new Dictionary<RavenServer, int>();
                foreach (var server in nodes)
                {
                    var shard = shardingConfig.Shards.First(x => x.Value.Members.Contains(server.ServerStore.NodeTag)).Key;
                    serverToShard[server] = shard;
                }

                var shardOnLeader = serverToShard[leader];
                
                var shard2User = "users/1";
                Assert.Equal(2, await Sharding.GetShardNumberForAsync(store, shard2User));
                var shard0User = "users/0";
                Assert.Equal(0, await Sharding.GetShardNumberForAsync(store, shard0User));
                var shard1User = "users/6";
                Assert.Equal(1, await Sharding.GetShardNumberForAsync(store, shard1User));

                //create 3 compare exchanges in cluster
                var cx1 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>(shard0User, user, 0));
                var cx2 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>(shard1User, user, 0));
                var lastDeletedCx = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>(shard2User, user, 0));

                var timeBeforeCxDeletion = DateTime.UtcNow;

                //delete cx to create tombstones
                cx1 = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>(shard0User, cx1.Index));
                cx2 = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>(shard1User, cx2.Index));
                await TriggerAClusterTransactionToForceCheckTombstonesAsync(store);
                
                //run periodic backup on all shards
                var config = Backup.CreateBackupConfiguration(backupPath, incrementalBackupFrequency: "0 0 1 * *");
                var backupTaskId = await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(leader, store, config, isFullBackup: false);

                var documentDatabase1 = await Cluster.GetAnyDocumentDatabaseInstanceFor(store, new List<RavenServer>() { nodes[1] }, ShardHelper.ToShardName(database, serverToShard[nodes[1]]));
                await WaitAndAssertForValueAsync(() => documentDatabase1.PeriodicBackupRunner.PeriodicBackups.FirstOrDefault(x => x.Configuration.TaskId == backupTaskId) != null, true);
                documentDatabase1.PeriodicBackupRunner.StartBackupTask(backupTaskId, isFullBackup: false);

                var documentDatabase2 = await Cluster.GetAnyDocumentDatabaseInstanceFor(store, new List<RavenServer>() { nodes[2] }, ShardHelper.ToShardName(database, serverToShard[nodes[2]]));
                await WaitAndAssertForValueAsync(() => documentDatabase2.PeriodicBackupRunner.PeriodicBackups.FirstOrDefault(x => x.Configuration.TaskId == backupTaskId) != null, true);
                documentDatabase2.PeriodicBackupRunner.StartBackupTask(backupTaskId, isFullBackup: false);
                
                //wait till all shards finish backing up the 2 tombstones
                foreach (var node in nodes)
                {
                    var finished = await WaitForValueAsync(() =>
                    {
                        using (node.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var itemName = PeriodicBackupStatus.GenerateItemName(ShardHelper.ToShardName(database, serverToShard[node]), backupTaskId);
                            var status = node.ServerStore.Cluster.Read(context, itemName);
                            if (status == null)
                                return false;
                            status.TryGet(nameof(LastRaftIndex), out BlittableJsonReaderObject lastRaftIndexBlittable);
                            lastRaftIndexBlittable.TryGet(nameof(LastRaftIndex.LastEtag), out long etag);
                            
                            return etag >= cx2.Index;
                        }
                    }, true);

                    Assert.True(finished);
                }

                //wait till tombstone cleaner cleans the 2 cx tombstones
                await WaitAndAssertForValueAsync(() => leader.ServerStore.Observer._lastTombstonesCleanupTimeInTicks > timeBeforeCxDeletion.Ticks, true);
                
                await AssertCompareExchangesAsync(database, expectedCompareExchanges: 1, expectedTombstones: 0, nodes);

                //suspend observer to stall tombstone cleaning
                leader.ServerStore.Observer.Suspended = true;

                //stall the periodic backup on 2 shards
                var tcs = new TaskCompletionSource<object>();
                var server1Database = await Sharding.GetAnyShardDocumentDatabaseInstanceFor(ShardHelper.ToShardName(database, serverToShard[nodes[1]]), new List<RavenServer>() { nodes[1] });
                server1Database.PeriodicBackupRunner.ForTestingPurposesOnly().OnBackupTaskRunHoldBackupExecution = tcs;
                var server2Database = await Sharding.GetAnyShardDocumentDatabaseInstanceFor(ShardHelper.ToShardName(database, serverToShard[nodes[2]]), new List<RavenServer>() { nodes[2] });
                server2Database.PeriodicBackupRunner.ForTestingPurposesOnly().OnBackupTaskRunHoldBackupExecution = tcs;

                timeBeforeCxDeletion = DateTime.UtcNow;

                //delete the last compare exchanges
                lastDeletedCx = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>(shard2User, lastDeletedCx.Index));
                await TriggerAClusterTransactionToForceCheckTombstonesAsync(store);

                
                //trigger periodic backup again on leader
                var documentDatabase = await Cluster.GetAnyDocumentDatabaseInstanceFor(store, new List<RavenServer>() {leader}, ShardHelper.ToShardName(database, shardOnLeader));
                documentDatabase.PeriodicBackupRunner.StartBackupTask(backupTaskId, isFullBackup: false);
                
                //wait for periodic backup to finish running
                var done = await WaitForValueAsync(() =>
                {
                    using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var itemName = PeriodicBackupStatus.GenerateItemName(ShardHelper.ToShardName(database, shardOnLeader), backupTaskId);
                        var status = leader.ServerStore.Cluster.Read(context, itemName);
                        if (status == null)
                            return false;
                        status.TryGet(nameof(LastRaftIndex), out BlittableJsonReaderObject lastRaftIndexBlittable);
                        lastRaftIndexBlittable.TryGet(nameof(LastRaftIndex.LastEtag), out long etag);
                        
                        return etag >= lastDeletedCx.Index;
                    }
                }, true);

                Assert.True(done);
                
                //unsuspend and wait for the tombstone cleaner
                leader.ServerStore.Observer.Suspended = false;
                
                //wait for the cleaner to execute
                await WaitAndAssertForValueAsync(() =>
                {
                    return leader.ServerStore.Observer._lastTombstonesCleanupTimeInTicks > timeBeforeCxDeletion.Ticks;
                }, true);

                //ensure last compare exchange tombstone wasn't deleted after the tombstone cleanup
                await AssertCompareExchangesAsync(database, expectedCompareExchanges: 0, expectedTombstones: 1, nodes);
            }
        }

        private class TestOjb
        {
            public string Id { get; set; }
        }
        private static async Task TriggerAClusterTransactionToForceCheckTombstonesAsync(IDocumentStore store)
        {
            using (var session = store.OpenAsyncSession(new SessionOptions{TransactionMode = TransactionMode.ClusterWide, DisableAtomicDocumentWritesInClusterWideTransaction = true}))
            {
                await session.StoreAsync(new TestOjb());
                await session.SaveChangesAsync();
            }
        }

        private async Task AssertCompareExchangesAsync(string database, int expectedCompareExchanges, int expectedTombstones, List<RavenServer> nodes)
        {
            foreach (var node in nodes)
            {
                long numOfCompareExchangeTombstones = -1;
                long numOfCompareExchanges = -1;
                
                await WaitForValueAsync(() =>
                {
                    using (node.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        numOfCompareExchangeTombstones = node.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, database);
                        numOfCompareExchanges = node.ServerStore.Cluster.GetNumberOfCompareExchange(context, database);
                        
                        return numOfCompareExchanges == expectedCompareExchanges && numOfCompareExchangeTombstones == expectedTombstones;
                    }
                }, true);

                Assert.Equal(expectedCompareExchanges, numOfCompareExchanges);
                Assert.Equal(expectedTombstones, numOfCompareExchangeTombstones);
            }
        }
    }
}
