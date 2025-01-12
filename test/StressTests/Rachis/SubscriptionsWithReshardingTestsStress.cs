using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Config;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using static SlowTests.Sharding.Cluster.SubscriptionsWithReshardingTests;

// ReSharper disable ClassNeverInstantiated.Local
// ReSharper disable CollectionNeverUpdated.Local
#pragma warning disable CS0649
#pragma warning disable CS0169

namespace StressTests.Rachis
{
    public class SubscriptionsWithReshardingTestsStress : ReplicationTestBase
    {
        public SubscriptionsWithReshardingTestsStress(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task ContinueSubscriptionAfterReshardingInAClusterRF3WithOrchestratorFailover()
        {
            var cluster = await CreateRaftCluster(5, watcherCluster: true, shouldRunInMemory: false);
            // 5 node, 2 orch, 3 shard, rf 3 
            // drop orch

            var first3 = cluster.Nodes.Select(x => x.ServerStore.NodeTag).Take(3).ToList();
            var last2 = cluster.Nodes.Select(x => x.ServerStore.NodeTag).Skip(3).Take(2).ToList();
            Assert.All(first3, x => Assert.DoesNotContain(x, last2));
            var ops = new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.Sharding ??= new ShardingConfiguration()
                    {
                        Shards = new Dictionary<int, DatabaseTopology>()
                        {
                            { 0, new DatabaseTopology() { ReplicationFactor = 3, Members = first3 } },
                            { 1, new DatabaseTopology() { ReplicationFactor = 3, Members = first3 } },
                            { 2, new DatabaseTopology() { ReplicationFactor = 3, Members = first3 } }
                        },
                        Orchestrator = new OrchestratorConfiguration { Topology = new OrchestratorTopology { ReplicationFactor = 2, Members = last2 } }
                    };
                },
                ReplicationFactor = 3,
                Server = cluster.Leader,
                RunInMemory = false
            };

            using var store = Sharding.GetDocumentStore(ops);
            var id = await store.Subscriptions.CreateAsync<User>(predicate: u => u.Age > 0);
            int.TryParse(id, out var id2);

            await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(id2, cluster.Nodes);

            var users = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            var t = Task.Run(async () => await ProcessSubscription(store, id, users, timoutSec: 60));

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1)))
            {
                var fail = Task.Run(async () =>
                {
                    (string DataDirectory, string Url, string NodeTag) result = default;
                    var recoveryOptions = new ServerCreationOptions
                    {
                        RunInMemory = false,
                        DeletePrevious = false,
                        RegisterForDisposal = true,
                        CustomSettings = DefaultClusterSettings
                    };
                    recoveryOptions.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] =
                        cluster.Leader.Configuration.Cluster.ElectionTimeout.AsTimeSpan.TotalMilliseconds.ToString();

                    while (cts.IsCancellationRequested == false)
                    {
                        var tag = last2[Random.Shared.Next(0, 2)];
                        var node = cluster.Nodes.First(x => x.ServerStore.NodeTag == tag);
                        if (node.ServerStore.IsLeader())
                            continue;

                        var position = cluster.Nodes.IndexOf(node);
                        result = await DisposeServerAndWaitForFinishOfDisposalAsync(node);

                        await Cluster.WaitForNodeToBeRehabAsync(store, result.NodeTag);
                        await Task.Delay(TimeSpan.FromSeconds(3));

                        Assert.Equal(cluster.Nodes[position].ServerStore.NodeTag, tag);
                        cluster.Nodes[position] = await ReviveNodeAsync(result, recoveryOptions);
                        await Cluster.WaitForAllNodesToBeMembersAsync(store);
                    }
                });

                try
                {
                    var added1 = await CreateItems(store, 0, 2);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    var added2 = await CreateItems(store, 2, 4, update: true);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    var added3 = await CreateItems(store, 4, 6, update: true);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    var added4 = await CreateItems(store, 6, 7, update: true);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    var added5 = await CreateItems(store, 7, 8, update: true);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    var added6 = await CreateItems(store, 8, 10, update: true);
                }
                finally
                {
                    cts.Cancel();
                    await fail;
                    await t;
                }
            }

            //  await PrintCollectionAndSubscriptionChangeVectors(store, cluster, id);

            await Indexes.WaitForIndexingInTheClusterAsync(store);
            using (var session = store.OpenAsyncSession())
            {
                var total = await session.Query<User>().CountAsync();
                Assert.Equal(195, total);

                var usersByQuery = await session.Query<User>().Where(u => u.Age > 0).ToListAsync();
                foreach (var user in usersByQuery)
                {
                    Assert.True(users.TryGetValue(user.Id, out var age), $"Missing {user.Id} from subscription");
                    Assert.True(age == user.Age, $"From sub:{age}, from shard: {user.Age} for {user.Id} cv:{session.Advanced.GetChangeVectorFor(user)}");
                    users.Remove(user.Id);
                }
            }

            await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id, cluster.Nodes);
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task GetDocumentsWithFilteringAndModifications2()
        {
            using var store = Sharding.GetDocumentStore();
            var id = await store.Subscriptions.CreateAsync<User>(predicate: u => u.Age > 0);
            var users = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            await CreateItems(store, 0, 2);
            await ProcessSubscription(store, id, users);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 2, 4, update: true);
            await ProcessSubscription(store, id, users);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 4, 6, update: true);
            await ProcessSubscription(store, id, users);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 6, 7, update: true);
            await ProcessSubscription(store, id, users);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 7, 8, update: true);
            await ProcessSubscription(store, id, users);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 9, 10, update: true);
            await ProcessSubscription(store, id, users);

            using (var session = store.OpenAsyncSession())
            {
                var total = await session.Query<User>().CountAsync();
                Assert.Equal(195, total);

                var usersByQuery = await session.Query<User>().Where(u => u.Age > 0).ToListAsync();
                foreach (var user in usersByQuery)
                {
                    Assert.True(users.TryGetValue(user.Id, out var age), $"Missing {user.Id} from subscription");
                    Assert.True(age == user.Age, $"From sub:{age}, from shard: {user.Age} for {user.Id} cv:{session.Advanced.GetChangeVectorFor(user)}");
                    users.Remove(user.Id);
                }
            }

            await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id);
        }

    }
}
