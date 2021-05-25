using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.DirectoryServices.ActiveDirectory;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Commands.Cluster;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Index = Raven.Server.Documents.Indexes.Index;

namespace SlowTests.Rolling
{
    public class RollingIndexesClusterTests : ClusterTestBase
    {
        public RollingIndexesClusterTests(ITestOutputHelper output) : base(output)
        {
        }

        private Task CreateData(IDocumentStore store)
        {
            return store.Maintenance.SendAsync(new CreateSampleDataOperation());
        }

        [Fact]
        public async Task DeployStaticRollingIndex()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3,
            }))
            {
                await CreateData(store);

                var count = 0L;
                foreach (var server in Servers)
                {
                    var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    var indexStore = database.IndexStore;
                    indexStore.ForTestingPurposesOnly().OnRollingIndexFinished = _ => Interlocked.Increment(ref count);
                }

                await store.ExecuteIndexAsync(new MyRollingIndex());

                WaitForIndexingInTheCluster(store, store.Database);

                await AssertWaitForValueAsync(() => Task.FromResult(count), 3L);
                
                VerifyHistory(cluster, store);
            }
        }

        [Fact]
        public async Task AddNewNodeWhileRollingIndexDeployed()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 2,
            }))
            {
                await CreateData(store);

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var topology = record.Topology;

                var count = 0L;
                var mre = new ManualResetEventSlim();
                foreach (var server in Servers)
                {
                    if (topology.RelevantFor(server.ServerStore.NodeTag) == false)
                    {
                        server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().AfterDatabaseCreation = db =>
                        {
                            db.IndexStore.ForTestingPurposesOnly().OnRollingIndexFinished = _ =>
                            {
                                Interlocked.Increment(ref count);
                            };
                        };
                        continue;
                    }

                    var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    var indexStore = database.IndexStore;
                    indexStore.ForTestingPurposesOnly().OnRollingIndexFinished = _ =>
                    {
                        Interlocked.Increment(ref count);
                    };
                    indexStore.ForTestingPurposesOnly().OnRollingIndexStart = _ =>
                    {
                        mre.Wait(database.DatabaseShutdown);
                    };
                }

                await store.ExecuteIndexAsync(new MyRollingIndex());

                await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database));

                mre.Set();

                var last = - 1L;
                await WaitAndAssertForValueAsync(
                    async () =>
                    {
                        var r = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                        last = r.Etag;
                        return r.Topology.Members.Count;
                    },
                    3);

                await WaitForRaftIndexToBeAppliedOnClusterNodes(last, Servers);
                WaitForIndexingInTheCluster(store, store.Database);

                await AssertWaitForValueAsync(() => Task.FromResult(count), 3L);

                VerifyHistory(cluster, store);
            } 
        }

        [Fact(Skip = "WIP")]
        public Task RollingIndexReplacementRetry()
        {
            return Task.CompletedTask;
        }


        // 1. create index
        // 2. modify it
        // 3. swap now
        [Fact(Skip = "WIP")]
        public Task RollingIndexReplcemantRetry2()
        {
            return Task.CompletedTask;
        }

        [Fact(Skip = "WIP")]
        public Task RollingIndexDeployedWithError()
        {
            return Task.CompletedTask;
        }

        [Fact(Skip = "WIP")]
        public Task RollingIndexDeployedSwapNow()
        {
            return Task.CompletedTask;
        }

        [Fact]
        public async Task EditRollingIndexDeployedWhileOldDeploymentInProgress()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3,
            }))
            {
                await CreateData(store);
                var mre = new ManualResetEventSlim();
                var dic = new ConcurrentDictionary<string, int>();
                foreach (var server in Servers)
                {
                    var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    var indexStore = database.IndexStore;
                    indexStore.ForTestingPurposesOnly().OnRollingIndexStart = _ => mre.Wait();
                    indexStore.ForTestingPurposesOnly().OnRollingIndexFinished = index =>
                    {
                        dic.AddOrUpdate(index.Name, 1, (_, val) => val + 1);
                    };
                }

                await store.ExecuteIndexAsync(new MyRollingIndex());
                await store.ExecuteIndexAsync(new MyEditedRollingIndex());

                mre.Set();

                WaitForIndexingInTheCluster(store, store.Database);

                await AssertWaitForValueAsync(() => Task.FromResult(dic.Keys.Count), 1);
                await AssertWaitForGreaterThanAsync(() => Task.FromResult(dic[nameof(MyRollingIndex)]), 2);

                VerifyHistory(cluster, store);
            }
        }

        [Fact]
        public async Task RollingIndexDeployedWhileNodeIsDown()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3,
            }))
            {
                await CreateData(store);

                await DisposeServerAndWaitForFinishOfDisposalAsync(cluster.Nodes.First(x => x != cluster.Leader));

                await store.ExecuteIndexAsync(new MyRollingIndex());

                using (cluster.Leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var record = cluster.Leader.ServerStore.Cluster.ReadDatabase(ctx, store.Database);
                    record.RollingIndexes.TryGetValue(nameof(MyRollingIndex), out var deployment);
                    var pending = deployment?.ActiveDeployments.Count(x => x.Value.State == RollingIndexState.Pending) ?? -1;
                    Assert.InRange(pending, 1, 2);
                }
            }
        }

        [Fact]
        public async Task ForceIndexDeployed()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3,
            }))
            {
                await CreateData(store);

                var mre = new ManualResetEventSlim();
                foreach (var server in Servers)
                {
                    var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    var indexStore = database.IndexStore;
                    indexStore.ForTestingPurposesOnly().OnRollingIndexStart = _ =>
                    {
                        mre.Wait(database.DatabaseShutdown);
                    };
                }

                await store.ExecuteIndexAsync(new MyRollingIndex());

                var result = await cluster.Leader.ServerStore.SendToLeaderAsync(new PutRollingIndexCommand(store.Database, nameof(MyRollingIndex), DateTime.UtcNow, RaftIdGenerator.DontCareId));
                var database1 = await cluster.Leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                await database1.RachisLogIndexNotifications.WaitForIndexNotification(result.Index, database1.DatabaseShutdown);

                VerifyHistory(cluster, store);
            }
        }

        [Fact]
        public async Task RemoveNodeFromClusterWhileRollingDeployment()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3,
                RunInMemory = false,
                DeleteDatabaseOnDispose = false // we removing a node, which will break the infra deletion, because we will bootstrap the removed node
            }))
            {
                await CreateData(store);

                var count = 0L;
                var mre = new ManualResetEventSlim();
                foreach (var server in Servers)
                {
                    var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    var indexStore = database.IndexStore;
                    indexStore.ForTestingPurposesOnly().OnRollingIndexFinished = _ =>
                    {
                        Interlocked.Increment(ref count);
                    };
                    indexStore.ForTestingPurposesOnly().OnRollingIndexStart = _ =>
                    {
                        mre.Wait(database.DatabaseShutdown);
                    };
                }

                await store.ExecuteIndexAsync(new MyRollingIndex());

                var node = cluster.Nodes.First(x => x != cluster.Leader);

                await store.Operations.SendAsync(new RemoveClusterNodeOperation(node.ServerStore.NodeTag));
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15)))
                {
                    await node.ServerStore.WaitForState(RachisState.Passive, cts.Token);
                }

                mre.Set();
                
                await AssertWaitForGreaterThanAsync(() => Task.FromResult(count), 1);

                VerifyHistoryAfterNodeRemoval(cluster, store);
            }
        }

        [Fact]
        public async Task RemoveNodeFromDatabaseGroupWhileRollingDeployment()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3, watcherCluster:true);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3,
                RunInMemory = false,
            }))
            {
                await CreateData(store);

                var count = 0L;
                var mre = new ManualResetEventSlim();
                foreach (var server in Servers)
                {
                    var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    var indexStore = database.IndexStore;
                    indexStore.ForTestingPurposesOnly().OnRollingIndexFinished = _ =>
                    {
                        Interlocked.Increment(ref count);
                    };

                    indexStore.ForTestingPurposesOnly().OnRollingIndexStart = index =>
                    {
                        mre.Wait(index.DocumentDatabase.DatabaseShutdown);
                    };
                }

                await store.ExecuteIndexAsync(new MyRollingIndex());

                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete: true, cluster.Leader.ServerStore.NodeTag, timeToWaitForConfirmation: TimeSpan.FromSeconds(15)));
                

                mre.Set();
                
                await AssertWaitForGreaterThanAsync(() => Task.FromResult(count), 1);

                VerifyHistoryAfterNodeRemoval(cluster, store);
            }
        }

        private static void VerifyHistoryAfterNodeRemoval((List<RavenServer> Nodes, RavenServer Leader) cluster, DocumentStore store)
        {
            using (cluster.Leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var record = cluster.Leader.ServerStore.Cluster.ReadDatabase(ctx, store.Database);
                var history = record.IndexesHistory;
                var deployment = history[nameof(MyRollingIndex)][0].RollingDeployment;
                Assert.Equal(2, deployment.Count);
                Assert.True(deployment.All(x => x.Value.State == RollingIndexState.Done));
                if (record.RollingIndexes?.ContainsKey(nameof(MyRollingIndex)) == true)
                    Assert.True(false, "RollingIndexes shouldn't contain 'MyRollingIndex'");
            }
        }

        private static void VerifyHistory((List<RavenServer> Nodes, RavenServer Leader) cluster, DocumentStore store)
        {
            using (cluster.Leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var record = cluster.Leader.ServerStore.Cluster.ReadDatabase(ctx, store.Database);
                var history = record.IndexesHistory;
                var deployment = history[nameof(MyRollingIndex)][0].RollingDeployment;
                Assert.Equal(3, deployment.Count);
                Assert.True(deployment.All(x => x.Value.State == RollingIndexState.Done));
                if (record.RollingIndexes?.ContainsKey(nameof(MyRollingIndex)) == true)
                    Assert.True(false, "RollingIndexes shouldn't contain 'MyRollingIndex'");
            }
        }

        public static Dictionary<string, RollingIndexDeployment> ReadDeployment(RavenServer server, string database, string index)
        {
            using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var record = server.ServerStore.Cluster.ReadDatabase(ctx, database);
                var history = record.IndexesHistory;
                return history[index][0].RollingDeployment;
            }
        }

        private static async Task<string> CreateIndex((List<RavenServer> Nodes, RavenServer Leader) cluster, string dbName)
        {
            var indexDefinition = new IndexDefinition { Name = "order_companies", Maps = { "from order in docs.Orders select new { company = order.Company }" }, DeploymentMode = IndexDeploymentMode.Rolling };

            var db = await cluster.Leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(dbName);

            await db.IndexStore.CreateIndexInternal(indexDefinition, Guid.NewGuid().ToString());

            foreach (var node in cluster.Nodes)
            {
                await WaitForRollingIndex(dbName, indexDefinition.Name, node);
            }

            return indexDefinition.Name;
        }

        private class MyRollingIndex : AbstractIndexCreationTask<Order>
        {
            public MyRollingIndex()
            {
                Map = orders => from order in orders
                    select new
                    {
                        order.Company,
                    };

                DeploymentMode = IndexDeploymentMode.Rolling;
            }
        }

        private class MyEditedRollingIndex : AbstractIndexCreationTask<Order>
        {
            public MyEditedRollingIndex()
            {
                Map = orders => from order in orders
                    select new
                    {
                        order.Company,
                        order.Employee
                    };

                DeploymentMode = IndexDeploymentMode.Rolling;
            }

            public override string IndexName => nameof(MyRollingIndex);
        }

        public static async Task<Index> WaitForRollingIndex(string database, string name, RavenServer server)
        {
            var db = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);

            while (true)
            {
                await Task.Delay(250);

                try
                {
                    var index = db.IndexStore.GetIndex(name);
                    if (index == null)
                        continue;
                    return index;
                }
                catch (PendingRollingIndexException)
                {
                }
            }
        }

        public static async Task WaitForRollingIndex(string database, string name, List<RavenServer> servers)
        {
            foreach (var server in servers)
            {
                await WaitForRollingIndex(database, name, server);
            }
        }

        private static async Task GenerateTestData(IDocumentStore leaderStore)
        {
            await leaderStore.Maintenance.SendAsync(new CreateSampleDataOperation());

            var operation = await leaderStore.Operations.SendAsync(new PatchByQueryOperation(@"FROM Orders UPDATE { put(""Orders/"", this); }"));

            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(2));
        }
    }
}
