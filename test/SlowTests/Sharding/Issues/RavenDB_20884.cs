using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues
{
    public class RavenDB_20884 : ReplicationTestBase
    {
        public RavenDB_20884(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        public async Task OnShardedDatabaseDeletion_ShouldDeleteNotificationsFromOrchestrator()
        {
            string databaseName;
            AlertRaised alert;

            ShardingConfiguration config;
            using (var store = Sharding.GetDocumentStore())
            {
                databaseName = store.Database;
                var shardedDbCtx = Sharding.GetOrchestrator(store.Database);
                config = Server.ServerStore.Cluster.ReadShardingConfiguration(databaseName);

                // store notification 
                var alertMsg = $"you have low disk space on node '{Server.ServerStore.NodeTag}'";
                alert = AlertRaised.Create(store.Database, "low disk space warning", alertMsg, AlertType.LowDiskSpace, NotificationSeverity.Warning);

                shardedDbCtx.NotificationCenter.Add(alert);

                using (shardedDbCtx.NotificationCenter.Storage.Read(alert.Id, out var ntv))
                {
                    Assert.NotNull(ntv);
                    Assert.True(ntv.Json.TryGet(nameof(AlertRaised.Message), out string msg), "Unable to read stored notification");
                    Assert.Equal(alertMsg, msg);
                }
            }

            if (config != null)
            {
                foreach (var name in config.Shards.Select(s => $"{databaseName}${s.Key}"))
                {
                    await AssertWaitForTrueAsync(() =>
                    {
                        var exists = Server.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(name, out _);
                        return Task.FromResult(exists == false);
                    });
                }
            }

            // recreate the sharded database
            using (var store2 = Sharding.GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => databaseName
            }))
            {
                var shardedDbCtx = Sharding.GetOrchestrator(store2.Database);

                // old notification should not be there
                using (shardedDbCtx.NotificationCenter.Storage.Read(alert.Id, out var ntv))
                {
                    Assert.Null(ntv);
                }
            }

        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        public async Task OnRemovingOrchestratorNode_ShouldDeleteShardedDatabaseNotificationsFromNode()
        {
            var cluster = await CreateRaftCluster(3);
            var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 1, orchestratorReplicationFactor: 3);

            using (var store = GetDocumentStore(options))
            {
                var server = cluster.Nodes.First(n => n != cluster.Leader);
                var shardedDbCtx = Sharding.GetOrchestrator(store.Database, server);

                // store notification 
                var alertMsg = $"you have low disk space on node '{Server.ServerStore.NodeTag}'";
                var alert = AlertRaised.Create(store.Database, "low disk space warning", alertMsg, AlertType.LowDiskSpace, NotificationSeverity.Warning);

                shardedDbCtx.NotificationCenter.Add(alert);

                using (shardedDbCtx.NotificationCenter.Storage.Read(alert.Id, out var ntv))
                {
                    Assert.NotNull(ntv);
                    Assert.True(ntv.Json.TryGet(nameof(AlertRaised.Message), out string msg), "Unable to read stored notification");
                    Assert.Equal(alertMsg, msg);
                }

                // remove one node from orchestrator topology
                await store.Maintenance.Server.SendAsync(new RemoveNodeFromOrchestratorTopologyOperation(store.Database, server.ServerStore.NodeTag));
                var shardingConfig = await Sharding.GetShardingConfigurationAsync(store);
                Assert.Equal(2, shardingConfig.Orchestrator.Topology.Count);
                Assert.DoesNotContain(server.ServerStore.NodeTag, shardingConfig.Orchestrator.Topology.AllNodes);

                // verify that orchestrator notifications are removed from this node
                using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var tableName = $"Notifications.{store.Database.ToLower()}";
                    var table = ctx.Transaction.InnerTransaction.OpenTable(Raven.Server.Documents.Schemas.Notifications.Current, tableName);
                    Assert.Null(table);
                }
            }
        }
    }
}
