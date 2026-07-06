using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;

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
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    // our local change vector is           RAFT:2, TRXN:10
                    // case 1: incoming change vector A:10, RAFT:3          -> update    (although it is a conflict) 
                    // case 2: incoming change vector A:10, RAFT:2          -> update    (although it is a conflict)
                    // case 3: incoming change vector A:10, RAFT:1          -> already merged
                    var remote = $"A:10-{databaseId}, RAFT:10-{raftId}";
                    var local = $"RAFT:10-{raftId}, TRXN:10-{clusterId}";
                    var status = database.DocumentsStorage.GetConflictStatusForVersion(context, remote, local);
                    Assert.Equal(ConflictStatus.Update, status);
                    status = database.DocumentsStorage.GetConflictStatusForVersion(context, local, remote);
                    Assert.Equal(ConflictStatus.AlreadyMerged, status);
                    local = $"A:10-{databaseId}, RAFT:11-{raftId}";
                    remote = $"RAFT:10-{raftId}, TRXN:10-{clusterId}";
                    status = database.DocumentsStorage.GetConflictStatusForVersion(context, remote, local);
                    Assert.Equal(ConflictStatus.AlreadyMerged, status);
                    status = database.DocumentsStorage.GetConflictStatusForVersion(context, local, remote);
                    Assert.Equal(ConflictStatus.Update, status);
                    remote = $"A:10-{databaseId}";
                    local = $"RAFT:10-{raftId}, TRXN:10-{clusterId}";
                    status = database.DocumentsStorage.GetConflictStatusForVersion(context, remote, local);
                    // this is conflict between cluster and non-cluster, we have a special treatment for this case higher in the stack
                    Assert.Equal(ConflictStatus.Conflict, status);
                    local = $"A:10-{unusedId}";
                    remote = $"RAFT:10-{raftId}, TRXN:10-{clusterId}";
                    status = database.DocumentsStorage.GetConflictStatusForVersion(context, local, remote);
                    Assert.Equal(ConflictStatus.Conflict, status);
                }
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
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
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
                    var status = database.DocumentsStorage.GetConflictStatusForVersion(context, remote, local);
                    Assert.Equal(ConflictStatus.Update, status);
                    remote = $"A:11-{databaseId}, B:10-{otherNode}, C:10-{unusedId}";
                    status = database.DocumentsStorage.GetConflictStatusForVersion(context, remote, local);
                    Assert.Equal(ConflictStatus.Update, status);
                    remote = $"A:11-{databaseId}, B:10-{otherNode}";
                    status = database.DocumentsStorage.GetConflictStatusForVersion(context, remote, local);
                    Assert.Equal(ConflictStatus.Update, status);
                    remote = $"A:10-{databaseId}, B:10-{otherNode}";
                    status = database.DocumentsStorage.GetConflictStatusForVersion(context, remote, local);
                    Assert.Equal(ConflictStatus.AlreadyMerged, status);
                    local = $"A:11-{databaseId}, B:10-{otherNode}";
                    remote = $"A:10-{databaseId}, B:10-{otherNode}, C:10-{unusedId}";
                    status = database.DocumentsStorage.GetConflictStatusForVersion(context, remote, local);
                    Assert.Equal(ConflictStatus.AlreadyMerged, status);
                    remote = $"A:10-{databaseId}, B:11-{otherNode}, C:10-{unusedId}";
                    status = database.DocumentsStorage.GetConflictStatusForVersion(context, remote, local);
                    Assert.Equal(ConflictStatus.Conflict, status);
                    remote = $"A:11-{databaseId}, B:10-{otherNode}, C:10-{unusedId}";
                    status = database.DocumentsStorage.GetConflictStatusForVersion(context, remote, local);
                    Assert.Equal(ConflictStatus.Update, status);
                    remote = $"A:11-{databaseId}, B:12-{otherNode}, C:10-{unusedId}";
                    status = database.DocumentsStorage.GetConflictStatusForVersion(context, remote, local);
                    Assert.Equal(ConflictStatus.Update, status);
                }
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Replication | RavenTestCategory.ClusterTransactions)]
        public async Task RecreateAfterClusterWideDeleteWithCleanedTombstoneShouldPropagate()
        {
            var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: true);
            var database = GetDatabaseName();
            await CreateDatabaseInCluster(database, replicationFactor: 2, leader.WebUrl);

            var other = nodes.Single(n => n != leader);

            using var storeA = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl },
                Conventions = { DisableTopologyUpdates = true } // pin all writes to node A
            }.Initialize();

            using var storeB = new DocumentStore
            {
                Database = database,
                Urls = new[] { other.WebUrl },
                Conventions = { DisableTopologyUpdates = true } // read node B directly
            }.Initialize();

            // 1. cluster-tx create -> CV {RAFT, TRXN}, flag FromClusterTransaction (on both nodes via Raft)
            using (var session = storeA.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new Person { Name = "Old" }, "foo/bar");
                await session.SaveChangesAsync();
            }

            // 2. cluster-tx delete -> tombstone {RAFT, TRXN} with FromClusterTransaction on BOTH nodes
            using (var session = storeA.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                session.Delete("foo/bar");
                await session.SaveChangesAsync();
            }

            // advance the etag past the tombstone (so the cleaner may remove it) and confirm the
            // delete fully replicated to B before we touch the cleaner
            using (var session = storeA.OpenAsyncSession())
            {
                await session.StoreAsync(new Person { Name = "marker" }, "marker/1");
                await session.SaveChangesAsync();
            }
            Assert.True(await WaitForDocumentInClusterAsync<Person>(nodes, database, "marker/1",
                p => p != null, TimeSpan.FromSeconds(30)));

            var dbA = await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);

            // 3. clean the DOCUMENT tombstone on node A ONLY (B keeps its cluster-tx tombstone)
            Assert.True(await WaitForValueAsync(async () =>
            {
                await dbA.TombstoneCleaner.ExecuteCleanup();
                using (dbA.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                using (DocumentIdWorker.GetLoweredIdSliceFromId(ctx, "foo/bar", out var lowerId))
                    return dbA.DocumentsStorage.GetDocumentOrTombstone(ctx, lowerId).Tombstone == null;
            }, expectedVal: true, timeout: 30_000));

            // 4. normal-tx recreate on A. No tombstone to inherit from -> CV is built from the
            //    TRXN-stripped database CV, so the recreate's CV has NO TRXN entry.
            using (var session = storeA.OpenAsyncSession())
            {
                await session.StoreAsync(new Person { Name = "New" }, "foo/bar");
                await session.SaveChangesAsync();
            }

            // 5. The recreate must propagate to B.
            var name = await WaitForValueAsync(async () =>
            {
                using var session = storeB.OpenAsyncSession();
                var p = await session.LoadAsync<Person>("foo/bar");
                return p?.Name;
            }, expectedVal: "New", timeout: 30_000);

            Assert.Equal("New", name); // FAILS: B stays deleted (name == null)
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [true])]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [false])]
        public async Task DeleteClusterWideAndNormalRecrateShouldPropagate(Options options, bool revisions)
        {
            using var src = GetDocumentStore(options);
            using var dst = GetDocumentStore(options);

            if (revisions)
            {
                await RevisionsHelper.SetupRevisionsAsync(src);
                await RevisionsHelper.SetupRevisionsAsync(dst);
            }

            using (var session = src.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                var user1 = new Person()
                {
                    Name = "Old",
                };
                await session.StoreAsync(user1, "foo/bar");
                await session.SaveChangesAsync();
            }

            using (var session = src.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                session.Delete("foo/bar");
                await session.SaveChangesAsync();
            }

            var cleanupState1 = await CompareExchangeTombstoneCleanerTestHelper.Clean(nodes: [Server], src.Database, ignoreClustrTrx: true);
            Assert.Equal(ClusterObserver.CompareExchangeTombstonesCleanupState.NoMoreTombstones, cleanupState1);

            await SetupReplicationAsync(src, dst);
            await EnsureReplicatingAsync(src, dst);

            using (var session = src.OpenAsyncSession())
            {
                var user1 = new Person()
                {
                    Name = "New",
                };
                await session.StoreAsync(user1, "foo/bar");
                await session.SaveChangesAsync();
            }

            await EnsureReplicatingAsync(src, dst);

            using (var session = dst.OpenAsyncSession())
            {
                var p = await session.LoadAsync<Person>("foo/bar");
                Assert.NotNull(p);
                Assert.Equal("New", p.Name);
            }

            using (var session = src.OpenAsyncSession())
            {
                var p = await session.LoadAsync<Person>("foo/bar");
                p.Name = "New2";
                await session.SaveChangesAsync();
            }
        }

        [RavenFact(RavenTestCategory.ClusterTransactions | RavenTestCategory.Replication)]
        public async Task DeleteClusterWideAndNormalRecrateShouldPropagateInternally()
        {
            var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: true);
            var database = GetDatabaseName();
            await CreateDatabaseInCluster(database, 2, leader.WebUrl);

            using var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl },
                Conventions = { DisableTopologyUpdates = true }
            }.Initialize();

            // hold internal replication from the leader to its sibling
            using var breakRepl = await BreakReplication(leader.ServerStore, database);

            using (var session = store.OpenAsyncSession())
            {
                var user1 = new Person()
                {
                    Name = "Old",
                };
                await session.StoreAsync(user1, "foo/bar");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                session.Delete("foo/bar");
                await session.SaveChangesAsync();
            }

            // release replication after the second write
            await breakRepl.MendAsync();

            using (var session = store.OpenAsyncSession())
            {
                var user1 = new Person()
                {
                    Name = "New",
                };
                await session.StoreAsync(user1, "foo/bar");
                await session.SaveChangesAsync();
            }

            Assert.True(await WaitForDocumentInClusterAsync<Person>(nodes, database, "foo/bar",
                p => p != null && p.Name == "New", TimeSpan.FromSeconds(30)));
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Replication | RavenTestCategory.ClusterTransactions)]
        public async Task ClusterWideWriteBackToRecreatedDocWithTombstonedGuardShouldSucceed()
        {
            var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: true);
            var database = GetDatabaseName();
            await CreateDatabaseInCluster(database, replicationFactor: 2, leader.WebUrl);

            using var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl },
                Conventions = { DisableTopologyUpdates = true }
            }.Initialize();

            // cluster-tx create -> live atomic guard
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new Person { Name = "Old" }, "foo/bar");
                await session.SaveChangesAsync();
            }

            // cluster-tx delete -> document and atomic guard tombstoned
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                session.Delete("foo/bar");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Person { Name = "marker" }, "marker/1");
                await session.SaveChangesAsync();
            }
            Assert.True(await WaitForDocumentInClusterAsync<Person>(nodes, database, "marker/1",
                p => p != null, TimeSpan.FromSeconds(30)));

            // drain the document tombstone so the recreate is born without a TRXN tag. Tombstone cleanup
            // has no client API, so this is the only server-side step; the marker write above advanced the
            // etag and confirmed the delete replicated, which makes the tombstone eligible for removal.
            var db = await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
            await db.TombstoneCleaner.ExecuteCleanup();

            // normal-tx recreate -> live doc whose CV lacks TRXN; its atomic guard is still a tombstone
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Person { Name = "New" }, "foo/bar");
                await session.SaveChangesAsync();
            }

            // the recreate (over a cleaned tombstone) has no TRXN tag
            using (var session = store.OpenAsyncSession())
            {
                var recreated = await session.LoadAsync<Person>("foo/bar");
                Assert.DoesNotContain("TRXN", session.Advanced.GetChangeVectorFor(recreated));
            }

            // a cluster-wide write-back to the recreated doc must succeed: the doc has no TRXN, so the guard
            // CAS uses index 0, and the deleted guard (treated as absent) accepts it.
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var p = await session.LoadAsync<Person>("foo/bar");
                Assert.NotNull(p);
                p.Name = "Updated";
                await session.SaveChangesAsync(); // must not throw ConcurrencyException
            }

            using (var session = store.OpenAsyncSession())
            {
                var p = await session.LoadAsync<Person>("foo/bar");
                Assert.Equal("Updated", p.Name);
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.ClusterTransactions)]
        public async Task ExistingConflictResolvedByRecreate_ExternalReplication()
        {
            Options NoAutoResolve() => new Options
            {
                ModifyDatabaseRecord = record => record.ConflictSolverConfig = new ConflictSolver
                {
                    ResolveToLatest = false,
                    ResolveByCollection = new Dictionary<string, ScriptResolver>()
                }
            };
            using var src = GetDocumentStore(NoAutoResolve());
            using var dst = GetDocumentStore(NoAutoResolve());

            // (a) src: cluster-tx create -> CV {RAFT, TRXN}
            using (var session = src.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new Person { Name = "src-val" }, "foo/bar");
                await session.SaveChangesAsync();
            }

            // (b) dst: an independent cluster-tx create in a different lineage -> CV {RAFT, TRXN}
            using (var session = dst.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new Person { Name = "dst-val" }, "foo/bar");
                await session.SaveChangesAsync();
            }

            // (c) replicate src->dst first: the two diverge, so dst stores them as a conflict (resolver disabled).
            var srcToDst = await SetupReplicationAsync(src, dst);
            var srcToDstTaskId = srcToDst.Single().TaskId;

            var conflicts = WaitUntilHasConflict(dst, "foo/bar", count: 2);
            Assert.Equal(2, conflicts.Length);
            // both stored conflict entries derive from a cluster-tx, so both carry TRXN
            Assert.All(conflicts, c => Assert.Contains("TRXN", c.ChangeVector));

            // now teach src dst's RAFT lineage (dst->src). src also stores the conflict (resolver off), which is
            // what we want: src's database CV absorbs dst's RAFT entry, so src's later recreate dominates BOTH
            // lineages. Wait until src is conflicted (i.e. it has seen dst's lineage) before building the recreate.
            await SetupReplicationAsync(dst, src);
            WaitUntilHasConflict(src, "foo/bar", count: 2);

            // (d) DELETE src->dst so only the FINAL recreate (not the intermediate delete tombstone) reaches dst.
            // Deleting (rather than disabling) also frees the tombstone cleaner: a disabled outgoing destination
            // still pins the minimal confirmed etag and would block the cleanup below.
            await DeleteOngoingTask(src as DocumentStore, srcToDstTaskId, OngoingTaskType.Replication);

            // delete foo/bar on src (normal tx), advance the etag + clean src's tombstone, then recreate (normal tx).
            // With no tombstone to inherit from, the recreate is born from src's TRXN-stripped database CV ->
            // its CV covers both lineages but has NO TRXN.
            using (var session = src.OpenAsyncSession())
            {
                session.Delete("foo/bar");
                await session.SaveChangesAsync();
            }

            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(new Person { Name = "marker" }, "marker/1");
                await session.SaveChangesAsync();
            }

            var srcDb = await Databases.GetDocumentDatabaseInstanceFor(src);
            Assert.True(await WaitForValueAsync(async () =>
            {
                await srcDb.TombstoneCleaner.ExecuteCleanup();
                using (srcDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                using (DocumentIdWorker.GetLoweredIdSliceFromId(ctx, "foo/bar", out var lowerId))
                    return srcDb.DocumentsStorage.GetDocumentOrTombstone(ctx, lowerId).Tombstone == null;
            }, expectedVal: true, timeout: 30_000));

            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(new Person { Name = "resolved" }, "foo/bar");
                await session.SaveChangesAsync();
            }

            // tripwire: the recreate must have lost its TRXN tag (no tombstone to inherit from), otherwise it
            // would carry TRXN and would not exercise the TRXN-stripping bug in the existing-conflicts loop.
            using (var session = src.OpenAsyncSession())
            {
                var recreated = await session.LoadAsync<Person>("foo/bar");
                Assert.DoesNotContain("TRXN", session.Advanced.GetChangeVectorFor(recreated));
            }

            // (e) Re-add src->dst. The recreate must resolve dst's stored conflict and converge to "resolved".
            // src's foo/bar tombstone was cleaned, so only the live "resolved" doc replicates.
            await SetupReplicationAsync(src, dst);

            var name = await WaitForValueAsync(async () =>
            {
                using var session = dst.OpenAsyncSession();
                try
                {
                    var p = await session.LoadAsync<Person>("foo/bar");
                    return p?.Name;
                }
                catch (Raven.Client.Exceptions.Documents.DocumentConflictException)
                {
                    return "<conflicted>";
                }
            }, expectedVal: "resolved", timeout: 60_000);

            Assert.Equal("resolved", name);
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
