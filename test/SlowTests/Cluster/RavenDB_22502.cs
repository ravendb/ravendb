using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
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

                await WaitForDocumentInClusterAsync<User>(nodes, store.Database, id, predicate: null, TimeSpan.FromSeconds(10));
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
                WaitForIndexingInTheCluster(store);

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

                await WaitForDocumentInClusterAsync<User>(nodes, store.Database, id, predicate: null, TimeSpan.FromSeconds(15));
                WaitForIndexingInTheCluster(store);
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
                
                await WaitForDocumentInClusterAsync<User>(nodes, store.Database, id, predicate: null, TimeSpan.FromSeconds(10));

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

                await WaitForDocumentInClusterAsync<User>(nodes, store.Database, id, predicate: null, TimeSpan.FromSeconds(15));

                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.SingleNode }))
                {
                    var u = await session.LoadAsync<User>(id);
                    Assert.NotNull(u);
                    u.Age = 10;
                    await session.SaveChangesAsync();
                }

                await WaitForDocumentInClusterAsync<User>(nodes, store.Database, id, predicate: u => u.Age == 10, TimeSpan.FromSeconds(15));

                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var u = await session.LoadAsync<User>(id);
                    Assert.NotNull(u);
                    u.Count = 10;
                    await session.SaveChangesAsync();
                }

                await WaitForDocumentInClusterAsync<User>(nodes, store.Database, id, predicate: u => u.Count == 10, TimeSpan.FromSeconds(15));
            }
        }
        
        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.ClusterTransactions)]
        public async Task ClusterTransactionConflictStatusMatrix()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                var raftId = database.DatabaseGroupId;
                var clusterId = database.ClusterTransactionId;
                var databaseId = database.DbBase64Id;
                var unusedId = Guid.NewGuid().ToBase64Unpadded();

                // our local change vector is           RAFT:2, TRXN:10
                // case 1: incoming change vector A:10, RAFT:3          -> update    (although it is a conflict) 
                // case 2: incoming change vector A:10, RAFT:2          -> update    (although it is a conflict)
                // case 3: incoming change vector A:10, RAFT:1          -> already merged

                var remote = $"A:10-{databaseId}, RAFT:10-{raftId}";
                var local = $"RAFT:10-{raftId}, TRXN:10-{clusterId}";
                var status = database.DocumentsStorage.GetConflictStatusForVersion(remote, local);
                Assert.Equal(ConflictStatus.Update, status);

                status = database.DocumentsStorage.GetConflictStatusForVersion(local, remote);
                Assert.Equal(ConflictStatus.AlreadyMerged, status);

                local = $"A:10-{databaseId}, RAFT:11-{raftId}";
                remote = $"RAFT:10-{raftId}, TRXN:10-{clusterId}";
                status = database.DocumentsStorage.GetConflictStatusForVersion(remote, local);
                Assert.Equal(ConflictStatus.AlreadyMerged, status);

                status = database.DocumentsStorage.GetConflictStatusForVersion(local, remote);
                Assert.Equal(ConflictStatus.Update, status);

                remote = $"A:10-{databaseId}";
                local = $"RAFT:10-{raftId}, TRXN:10-{clusterId}";
                status = database.DocumentsStorage.GetConflictStatusForVersion(remote, local);
                // this is conflict between cluster and non-cluster, we have a special treatment for this case higher in the stack
                Assert.Equal(ConflictStatus.Conflict, status);

                local = $"A:10-{unusedId}";
                remote = $"RAFT:10-{raftId}, TRXN:10-{clusterId}";
                status = database.DocumentsStorage.GetConflictStatusForVersion(local, remote);
                Assert.Equal(ConflictStatus.Conflict, status);
            }
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task ConflictStatusMatrix()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                var databaseId = database.DbBase64Id;
                var otherNode = Guid.NewGuid().ToBase64Unpadded();
                var unusedId = Guid.NewGuid().ToBase64Unpadded();

                database.DocumentsStorage.UnusedDatabaseIds = [unusedId];

                // our local change vector is     A:10, B:10, C:10
                // case 1: incoming change vector A:10, B:10, C:11  -> update           (original: update, after: already merged)
                // case 2: incoming change vector A:11, B:10, C:10  -> update           (original: update, after: update)
                // case 3: incoming change vector A:11, B:10        -> update           (original: conflict, after: update)
                // case 4: incoming change vector A:10, B:10        -> already merged   (original: already merged, after: already merged)

                // our local change vector is     A:11, B:10
                // case 1: incoming change vector A:10, B:10, C:10 -> conflict              (original: conflict, after: already merged)        
                // case 2: incoming change vector A:10, B:11, C:10 -> conflict              (original: conflict, after: conflict)
                // case 3: incoming change vector A:11, B:10, C:10 -> update                (original: update, after: already merged)
                // case 4: incoming change vector A:11, B:12, C:10 -> update 

                var local = $"A:10-{databaseId}, B:10-{otherNode}, C:10-{unusedId}";
                var remote = $"A:10-{databaseId}, B:10-{otherNode}, C:11-{unusedId}"; 
                var status = database.DocumentsStorage.GetConflictStatusForVersion(remote, local);
                Assert.Equal(ConflictStatus.Update, status);

                remote = $"A:11-{databaseId}, B:10-{otherNode}, C:10-{unusedId}"; 
                status = database.DocumentsStorage.GetConflictStatusForVersion(remote, local);
                Assert.Equal(ConflictStatus.Update, status);

                remote = $"A:11-{databaseId}, B:10-{otherNode}"; 
                status = database.DocumentsStorage.GetConflictStatusForVersion(remote, local);
                Assert.Equal(ConflictStatus.Update, status);

                remote = $"A:10-{databaseId}, B:10-{otherNode}"; 
                status = database.DocumentsStorage.GetConflictStatusForVersion(remote, local);
                Assert.Equal(ConflictStatus.AlreadyMerged, status);


                local = $"A:11-{databaseId}, B:10-{otherNode}";
                remote = $"A:10-{databaseId}, B:10-{otherNode}, C:10-{unusedId}"; 
                status = database.DocumentsStorage.GetConflictStatusForVersion(remote, local);
                Assert.Equal(ConflictStatus.AlreadyMerged, status);

                remote = $"A:10-{databaseId}, B:11-{otherNode}, C:10-{unusedId}"; 
                status = database.DocumentsStorage.GetConflictStatusForVersion(remote, local);
                Assert.Equal(ConflictStatus.Conflict, status);

                remote = $"A:11-{databaseId}, B:10-{otherNode}, C:10-{unusedId}"; 
                status = database.DocumentsStorage.GetConflictStatusForVersion(remote, local);
                Assert.Equal(ConflictStatus.Update, status);

                remote = $"A:11-{databaseId}, B:12-{otherNode}, C:10-{unusedId}"; 
                status = database.DocumentsStorage.GetConflictStatusForVersion(remote, local);
                Assert.Equal(ConflictStatus.Update, status);
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
                /*@new.ModifyDatabaseRecord = r =>
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
                };*/
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
