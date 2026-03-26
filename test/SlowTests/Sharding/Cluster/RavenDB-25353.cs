using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.ServerWide.Commands.Sharding;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Sharding.Cluster
{
    public class RavenDB_25353 : ClusterTestBase
    {
        public RavenDB_25353(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task Resharding_Simple_Move_1_To_0()
        {
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Replication.MaxItemsCount)] = 1.ToString();

            var (nodes, leader) = await CreateRaftCluster(4, watcherCluster: true);
            var dbName = GetDatabaseName();

            var o = new Options
            {
                Server = leader,
                DatabaseMode = RavenDatabaseMode.Sharded,
                ModifyDatabaseRecord = r => r.Sharding = new ShardingConfiguration
                {
                    Orchestrator = new OrchestratorConfiguration
                    {
                        Topology = new OrchestratorTopology
                        {
                            Members = new List<string> { "A", "B", "C", "D" },
                        }
                    },
                    Shards = new Dictionary<int, DatabaseTopology>
                    {
                        {
                            0, new DatabaseTopology
                            {
                                Members = new List<string> { "A", "B" }
                            }
                        },
                        {
                            1, new DatabaseTopology
                            {
                                Members = new List<string> { "D", "C" }
                            }
                        }
                    }
                }
            };

            using (var store = GetDocumentStore(o, dbName))
            {
                var id = "items/0$items/anchor";
                var bucketToMove = 0;
                using (var session = store.OpenAsyncSession())
                {
                    bucketToMove = await Sharding.GetBucketAsync(store, id);
                    await session.StoreAsync(new User { Name = $"User 0" }, id);
                    await session.SaveChangesAsync();
                }

                var SrcShard = await Sharding.GetShardNumberForAsync(store, id);
                for (int i = 0; i < 5; i++)
                {
                    var destShard = SrcShard == 1 ? 0 : 1;
                    var bringBackNode = SrcShard == 1 ? "D" : "B";

                    await MoveBucketAndDeleteSourceNodeAsync(store, leader, bucketToMove, SrcShard, destShard, store.Database, bringBackNode);

                    await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database, SrcShard, bringBackNode));

                    var added = await WaitForValueAsync(async () =>
                    {
                        var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                        var topology = res.Sharding.Shards[SrcShard];

                        return topology.Members.Contains(bringBackNode);
                    }, true, timeout: 70_000);
                    Assert.True(added, $"didn't revive node {bringBackNode} on time");

                    SrcShard = destShard;
                }
            }
        }

        private async Task MoveBucketAndDeleteSourceNodeAsync(DocumentStore store, RavenServer leader, int bucket, int src, int dest, string dbName, string bringBackNode)
        {
            var moveCmd = new StartBucketMigrationCommand(
                bucket,
                src,
                dest,
                store.Database,
                prefix: null,
                raftId: RaftIdGenerator.NewId());

            var result = await leader.ServerStore.SendToLeaderAsync(moveCmd);

            var shardDbName = $"{dbName}${src}";

            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Anchor for migration" }, "items/0$items/anchor");
                    session.SaveChanges();
                    store.Maintenance.Server.Send(new DeleteDatabasesOperation(
                        shardDbName,
                        hardDelete: true,
                        fromNode: bringBackNode,
                        timeToWaitForConfirmation: TimeSpan.FromSeconds(10)
                    ));
                }

                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(result.Index, TimeSpan.FromSeconds(15));
                var finished = await WaitForValueAsync(async () =>
                {
                    var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    return record.Sharding.BucketMigrations.Count == 0;
                }, expectedVal: true, timeout: 15_000);

                Assert.True(finished, "The migration didn't finish in time.");
            }
        }

        private class User
        {
            public string Name { get; set; }
        }
    }
}
