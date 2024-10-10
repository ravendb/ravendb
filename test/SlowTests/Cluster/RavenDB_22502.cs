using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Cluster
{
    public class RavenDB_22502 : ReplicationTestBase
    {
        public RavenDB_22502(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ModifyClusterWideDocumentInNotUpToDateNode(Options options)
        {
            var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 3, watcherCluster: true, leaderIndex: 0);
            var database = GetDatabaseName();

            var o1 = options.Clone();
            ModifyTopology(options, o1);
            o1.ModifyDatabaseName = _ => database;
            o1.Server = leader;
            o1.DeleteDatabaseOnDispose = false;
            var id = "users/1";

            using (var store = GetDocumentStore(o1))
            {
                var watchers = nodes.Where(n => n != leader).Select(n => n.ServerStore.NodeTag).ToList();
                leader.ServerStore.Engine.ForTestingPurposesOnly().NodeTagsToDisconnect = watchers;

                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1)))
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    await session.StoreAsync(new User(), id, cts.Token);
                    try
                    {
                        await session.SaveChangesAsync(cts.Token);
                    }
                    catch (TaskCanceledException)
                    {
                        // can happen if we send the request to a watcher
                    }
                }

                await Cluster.WaitForDocumentOnAllNodesAsync<User>(store, id, predicate: null, TimeSpan.FromSeconds(10));
            }

            var o2 = options.Clone();
            o2.ModifyDatabaseName = _ => database;
            o2.Server = nodes.First(n => n != leader);
            o2.CreateDatabase = false;

            using (var store = GetDocumentStore(o2))
            {
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3)))
                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(id, cts.Token);
                    u.Age = 1;
                    try
                    {
                        await Assert.ThrowsAsync<TaskCanceledException>(() => session.SaveChangesAsync(cts.Token));
                    }
                    finally
                    {
                        leader.ServerStore.Engine.ForTestingPurposesOnly().NodeTagsToDisconnect = null;
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ModifyQueriedClusterWideDocumentInNotUpToDateNode(Options options)
        {
            var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 3, watcherCluster: true, leaderIndex: 0);
            var database = GetDatabaseName();

            var o1 = options.Clone();
            ModifyTopology(options, o1);

            o1.ModifyDatabaseName = _ => database;
            o1.Server = leader;
            o1.DeleteDatabaseOnDispose = false;
            var id = "users/1";

            using (var store = GetDocumentStore(o1))
            {
                await store.ExecuteIndexAsync(new MyUsers());
                await Indexes.WaitForIndexingAsync(store);

                var watchers = nodes.Where(n => n != leader).Select(n => n.ServerStore.NodeTag).ToList();
                leader.ServerStore.Engine.ForTestingPurposesOnly().NodeTagsToDisconnect = watchers;

                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1)))
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    await session.StoreAsync(new User(), id, cts.Token);
                    try
                    {
                        await session.SaveChangesAsync(cts.Token);
                    }
                    catch (TaskCanceledException)
                    {
                        // can happen if we send the request to a watcher
                    }
                }

                await Cluster.WaitForDocumentOnAllNodesAsync<User>(store, id, predicate: null, TimeSpan.FromSeconds(10));
                await Indexes.WaitForIndexingAsync(store);
            }

            var o2 = options.Clone();
            o2.ModifyDatabaseName = _ => database;
            o2.Server = nodes.First(n => n != leader);
            o2.CreateDatabase = false;

            using (var store = GetDocumentStore(o2))
            {
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3)))
                using (var session = store.OpenAsyncSession())
                {
                    var r = session.Query<User>("MyUsers").ToListAsync(cts.Token);
                    var u = r.Result.Single();
                    u.Age = 1;
                    try
                    {
                        await Assert.ThrowsAsync<TaskCanceledException>(() => session.SaveChangesAsync(cts.Token));
                    }
                    finally
                    {
                        leader.ServerStore.Engine.ForTestingPurposesOnly().NodeTagsToDisconnect = null;
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task DeletedClusterWideAndRecreateInNormalTx(Options options)
        {
            var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 3, watcherCluster: true, leaderIndex: 0);
            var database = GetDatabaseName();

            var o1 = options.Clone();
            ModifyTopology(options, o1);

            o1.ModifyDatabaseName = _ => database;
            o1.Server = leader;
            var id = "users/1";

            using (var store = GetDocumentStore(o1))
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    await session.StoreAsync(new User(), id);
                    await session.SaveChangesAsync();
                }
                
                await Cluster.WaitForDocumentOnAllNodesAsync<User>(store, id, predicate: null, TimeSpan.FromSeconds(10));

                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Delete(id);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.SingleNode }))
                {
                    var u = await session.LoadAsync<User>(id);
                    Assert.Null(u);

                    await session.StoreAsync(new User(), id);
                    await session.SaveChangesAsync();
                }

                await Cluster.WaitForDocumentOnAllNodesAsync<User>(store, id, predicate: null, TimeSpan.FromSeconds(10));

                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.SingleNode }))
                {
                    var u = await session.LoadAsync<User>(id);
                    Assert.NotNull(u);
                    u.Age = 10;
                    await session.SaveChangesAsync();
                }

                await Cluster.WaitForDocumentOnAllNodesAsync<User>(store, id, predicate: null, TimeSpan.FromSeconds(10));

                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var u = await session.LoadAsync<User>(id);
                    Assert.NotNull(u);
                    u.Count = 10;
                    await session.SaveChangesAsync();
                }

                await Cluster.WaitForDocumentOnAllNodesAsync<User>(store, id, predicate: null, TimeSpan.FromSeconds(10));
            }
        }
        
        private static void ModifyTopology(Options original, Options @new)
        {
            if (original.DatabaseMode == RavenDatabaseMode.Single)
            {
                @new.ModifyDatabaseRecord = r => r.Topology = new DatabaseTopology
                {
                    Members = ["C", "B", "A"]
                };
            }
            else
            {
                @new.ModifyDatabaseRecord = r =>
                {
                    r.Sharding = new ShardingConfiguration
                    {
                        Shards = new Dictionary<int, DatabaseTopology>
                        {
                            [0] = new DatabaseTopology
                            {
                                Members = ["C", "B", "A"]
                            },
                            [1] = new DatabaseTopology
                            {
                                Members = ["C", "B", "A"]
                            },
                            [2] = new DatabaseTopology
                            {
                                Members = ["C", "B", "A"]
                            },
                        },
                      
                        Orchestrator = new OrchestratorConfiguration
                        {
                            Topology = new OrchestratorTopology
                            {
                                Members = ["C", "B", "A"]
                            }
                        }
                    };
                };
            }
        }

        private class MyUsers : AbstractIndexCreationTask<User>
        {
            public override string IndexName => "MyUsers";

            public MyUsers()
            {
                Map = users => from user in users
                    select new
                    {
                        user.Name
                    };
            }
        }
    }
}
