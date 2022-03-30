using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Orders;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
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

        private async Task CreateData(IDocumentStore store)
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation());
            var res = await WaitForDocumentInClusterAsync<Order>(store.GetRequestExecutor().TopologyNodes, "orders/830-A", predicate: null, TimeSpan.FromSeconds(15));
            Assert.True(res);
        }

        [Fact]
        public async Task DeployStaticRollingIndex()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStoreForRollingIndexes(new Options
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

                await VerifyHistory(cluster, store);
            }
        }

        [Fact]
        public async Task DeployRollingIndexWhileDocumentsModified()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStoreForRollingIndexes(
                       new Options
                       {
                           Server = cluster.Leader,
                           ReplicationFactor = 3,
                       }))
            {
                await CreateData(store);


                var count = 0L;
                var violation = new StringBuilder();

                foreach (var server in Servers)
                {
                    var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    var indexStore = database.IndexStore;
                    indexStore.ForTestingPurposesOnly().OnRollingIndexStart = index =>
                    {
                        var inc = Interlocked.Increment(ref count);
                        if (inc > 1)
                            violation.AppendLine($"{index} started concurrently (count: {inc})");
                    };
                    indexStore.ForTestingPurposesOnly().BeforeRollingIndexFinished = index =>
                    {
                        var dec = Interlocked.Decrement(ref count);
                        if (dec != 0)
                            violation.AppendLine($"finishing {index} must be zero but is {dec}");
                    };
                }
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                {
                    var t = ContinuouslyModifyDocuments(store, cts.Token);
                    try
                    {
                        await store.ExecuteIndexAsync(new MyRollingIndex());

                        WaitForIndexingInTheCluster(store, store.Database);

                        var v = violation.ToString();
                        Assert.True(string.IsNullOrEmpty(v), v);

                        await VerifyHistory(cluster, store);
                    }
                    finally
                    {
                        try
                        {
                            cts.Cancel();
                            await t;
                        }
                        catch
                        {
                            // ignore
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task EditRollingIndexWhileDocumentsModified()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStoreForRollingIndexes(
                       new Options
                       {
                           Server = cluster.Leader,
                           ReplicationFactor = 3,
                       }))
            {
                await CreateData(store);

                await store.ExecuteIndexAsync(new MyRollingIndex());

                WaitForIndexingInTheCluster(store, store.Database);

                await VerifyHistory(cluster, store);

                var count = 0L;
                var violation = new StringBuilder();

                foreach (var server in Servers)
                {
                    var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    var indexStore = database.IndexStore;
                    indexStore.ForTestingPurposesOnly().OnRollingIndexStart = index =>
                    {
                        if (index.Name != "ReplacementOf/MyRollingIndex")
                            return;

                        var inc = Interlocked.Increment(ref count);
                        if (inc > 1)
                            violation.AppendLine($"{index} started concurrently (count: {inc})");
                    };
                    indexStore.ForTestingPurposesOnly().BeforeRollingIndexFinished = index =>
                    {
                        var dec = Interlocked.Decrement(ref count);
                        if (dec != 0)
                            violation.AppendLine($"finishing {index} must be zero but is {dec} @ {server.ServerStore.NodeTag}");
                    };
                }

                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                {
                    var t = ContinuouslyModifyDocuments(store, cts.Token);
                    try
                    {
                        await store.ExecuteIndexAsync(new MyEditedRollingIndex());

                        WaitForIndexingInTheCluster(store, store.Database);

                        var v = violation.ToString();
                        Assert.True(string.IsNullOrEmpty(v), v);

                        await VerifyHistory(cluster, store);
                    }
                    finally
                    {
                        try
                        {
                            cts.Cancel();
                            await t;
                        }
                        catch
                        {
                            // ignore
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task EditRollingIndexMultipleTimesWhileDocumentsModified()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStoreForRollingIndexes(
                       new Options
                       {
                           Server = cluster.Leader,
                           ReplicationFactor = 3,
                       }))
            {
                await CreateData(store);

                await store.ExecuteIndexAsync(new MyRollingIndex());

                WaitForIndexingInTheCluster(store, store.Database);

                await VerifyHistory(cluster, store);

                var runningIndexes = new ConcurrentSet<Index>();
                var violation = new StringBuilder();
                var mre = new ManualResetEventSlim();
                foreach (var server in Servers)
                {
                    var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    var indexStore = database.IndexStore;

                    indexStore.ForTestingPurposesOnly().BeforeRollingIndexStart = index => mre.Wait(index.IndexingProcessCancellationToken);

                    indexStore.ForTestingPurposesOnly().OnRollingIndexStart = index =>
                    {
                        if (index.Name != "ReplacementOf/MyRollingIndex")
                            return;

                        if (runningIndexes.TryAdd(index) == false)
                        {
                            violation.AppendLine($"{index} already exists");
                        }

                        var inc = runningIndexes.Count;
                        if (inc > 1)
                        {
                            violation.AppendLine($"{index} started concurrently (count: {inc})");
                        }
                    };
                    indexStore.ForTestingPurposesOnly().BeforeRollingIndexFinished = index =>
                    {
                        if (runningIndexes.TryRemove(index) == false)
                        {
                            violation.AppendLine($"{index} isn't found");
                        }

                        var dec = runningIndexes.Count;
                        if (dec != 0)
                            violation.AppendLine($"finishing {index} must be zero but is {dec} @ {server.ServerStore.NodeTag}");
                    };

                    indexStore.ForTestingPurposesOnly().BeforeIndexThreadExit = index =>
                    {
                        if (index.IndexingProcessCancellationToken.IsCancellationRequested)
                            runningIndexes.TryRemove(index);
                    };
                }

                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                {
                    var t = ContinuouslyModifyDocuments(store, cts.Token);
                    try
                    {
                        try
                        {
                            await store.ExecuteIndexAsync(new MyEditedRollingIndex());
                            await store.ExecuteIndexAsync(new MyEditedRollingIndex2());
                            await store.ExecuteIndexAsync(new MyEditedRollingIndex());
                            await store.ExecuteIndexAsync(new MyEditedRollingIndex2());
                        }
                        finally
                        {
                            mre.Set();
                        }

                        WaitForIndexingInTheCluster(store, store.Database);

                        var v = violation.ToString();
                        Assert.True(string.IsNullOrEmpty(v), v);

                        await VerifyHistory(cluster, store);
                    }
                    finally
                    {
                        try
                        {
                            cts.Cancel();
                            await t;
                        }
                        catch
                        {
                            // ignore
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task RollingIndexSetAsDoneWhenNotDone_RDBS_8983()
        {
            var mre = new ManualResetEventSlim();
            var mre2 = new ManualResetEventSlim();
            var source = new CancellationTokenSource();
            var token = source.Token;

            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStoreForRollingIndexes(
                       new Options
                       {
                           Server = cluster.Leader,
                           ReplicationFactor = 3,
                       }))
            {
                await CreateData(store);
                await store.ExecuteIndexAsync(new MyRollingIndex() { DeploymentMode = IndexDeploymentMode.Parallel });
                WaitForIndexingInTheCluster(store, store.Database);

                foreach (var server in Servers)
                {
                    var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    var indexStore = database.IndexStore;

                    indexStore.ForTestingPurposesOnly().BeforeIndexStart = index =>
                    {
                        if (index.Definition.Name.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix))
                        {
                            mre.Set();
                            mre2.Wait(token);
                        }
                    };
                }
                var t = store.ExecuteIndexAsync(new MyEditedRollingIndex());
                Assert.True(mre.Wait(TimeSpan.FromMinutes(1)));
                while (t.IsCompleted == false)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Order { Company = "Toli" });
                        await session.SaveChangesAsync();
                    }
                    await Task.Delay(TimeSpan.FromMilliseconds(500));
                }

                try
                {
                    await AssertWaitForValueAsync(() =>
                    {
                        using (cluster.Leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        {
                            var record = cluster.Leader.ServerStore.Cluster.ReadDatabase(ctx, store.Database);
                            var history = record.IndexesHistory;
                            var deployment = history[nameof(MyRollingIndex)][0].RollingDeployment;


                            return Task.FromResult(deployment.Any(x => x.Value.State == RollingIndexState.Done));
                        }
                    }, false, 5000);
                }
                finally
                {
                    source.Cancel();
                }
            }
        }

        private async Task ContinuouslyModifyDocuments(IDocumentStore store, CancellationToken token)
        {
            while (token.IsCancellationRequested == false)
            {
                try
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var o = await session.LoadAsync<Order>("orders/830-A");
                        o.RequireAt = DateTime.UtcNow;
                        await session.SaveChangesAsync(token);
                    }
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch
                {
                    await Task.Delay(500, token);
                }
            }
        }

        [Fact]
        public async Task AddNewNodeWhileRollingIndexDeployed()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStoreForRollingIndexes(new Options
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
                            db.Database.IndexStore.ForTestingPurposesOnly().OnRollingIndexFinished = _ =>
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

                var last = -1L;
                await WaitAndAssertForValueAsync(
                    async () =>
                    {
                        var r = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                        last = r.Etag;
                        return r.Topology.Members.Count;
                    },
                    3);

                await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(last, Servers);

                var reqEx = store.GetRequestExecutor();
                var anyNode = await reqEx.GetPreferredNode();
                await reqEx.UpdateTopologyAsync(
                    new RequestExecutor.UpdateTopologyParameters(anyNode.Item2) { TimeoutInMs = 5000, ForceUpdate = true, DebugTag = "node-unavailable-exception" });

                WaitForIndexingInTheCluster(store, store.Database);

                await AssertWaitForValueAsync(() => Task.FromResult(count), 3L);

                await VerifyHistory(cluster, store);
            }
        }

        [Fact]
        public async Task EditRollingIndexDeployedWhileOldDeploymentInProgress()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStoreForRollingIndexes(new Options
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
                    indexStore.ForTestingPurposesOnly().OnRollingIndexStart = _ => Assert.True(mre.Wait(TimeSpan.FromMinutes(1)));
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

                await VerifyHistory(cluster, store);
            }
        }

        [Fact]
        public async Task RollingIndexDeployedWhileNodeIsDown()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStoreForRollingIndexes(new Options
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
            using (var store = GetDocumentStoreForRollingIndexes(new Options
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

                await VerifyHistory(cluster, store);

                mre.Set();
            }
        }

        [Fact]
        public async Task RemoveNodeFromClusterWhileRollingDeployment()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3);
            using (var store = GetDocumentStoreForRollingIndexes(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3,
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

                RavenServer node = default;
                await ActionWithLeader(leader =>
                {
                    node = cluster.Nodes.First(x => x != leader);
                    return leader.ServerStore.RemoveFromClusterAsync(node.ServerStore.NodeTag);
                });

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
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStoreForRollingIndexes(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3,
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


        [Fact]
        public async Task RollingIndexReplacementRetryWithUnauthorizedAccessException()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStoreForRollingIndexes(new Options
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

                    indexStore.ForTestingPurposesOnly().DuringIndexReplacement_AfterUpdatingCollectionOfIndexes = () =>
                    {
                        indexStore.ForTestingPurposesOnly().DuringIndexReplacement_AfterUpdatingCollectionOfIndexes = null;
                        throw new UnauthorizedAccessException("Simulate UnauthorizedAccessException AfterUpdatingCollectionOfIndexes Once");
                    };

                    indexStore.ForTestingPurposesOnly().DuringIndexReplacement_OnOldIndexDeletion = () =>
                    {
                        indexStore.ForTestingPurposesOnly().DuringIndexReplacement_OnOldIndexDeletion = null;
                        throw new UnauthorizedAccessException("Simulate UnauthorizedAccessException OnOldIndexDeletion Once");
                    };

                    indexStore.ForTestingPurposesOnly().OnRollingIndexStart = _ => Assert.True(mre.Wait(TimeSpan.FromMinutes(1)));
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

                await VerifyHistory(cluster, store);
            }
        }


        [Fact]
        public async Task RollingIndexReplacementRetryWithIOException()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStoreForRollingIndexes(new Options
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

                    indexStore.ForTestingPurposesOnly().DuringIndexReplacement_AfterUpdatingCollectionOfIndexes = () =>
                    {
                        indexStore.ForTestingPurposesOnly().DuringIndexReplacement_AfterUpdatingCollectionOfIndexes = null;
                        throw new IOException("Simulate IOException AfterUpdatingCollectionOfIndexes Once");
                    };

                    indexStore.ForTestingPurposesOnly().DuringIndexReplacement_OnOldIndexDeletion = () =>
                    {
                        indexStore.ForTestingPurposesOnly().DuringIndexReplacement_OnOldIndexDeletion = null;
                        throw new IOException("Simulate IOException OnOldIndexDeletion Once");
                    };

                    indexStore.ForTestingPurposesOnly().OnRollingIndexStart = _ => Assert.True(mre.Wait(TimeSpan.FromMinutes(1)));
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
                await AssertWaitForGreaterThanAsync(() => Task.FromResult(dic[nameof(MyRollingIndex)]), 2, timeout: (int)TimeSpan.FromSeconds(3600).TotalMilliseconds);

                await VerifyHistory(cluster, store);
            }
        }

        [Fact]
        public async Task RollingIndexDeployedWithError()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStoreForRollingIndexes(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3,
            }))
            {
                await CreateData(store);

                var count = 0L;
                var info = "";
                foreach (var server in Servers)
                {
                    var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    var indexStore = database.IndexStore;
                    indexStore.ForTestingPurposesOnly().OnRollingIndexFinished = index =>
                    {
                        Interlocked.Increment(ref count);
                        info += $"Server : Count = {count}. {index.DocumentDatabase.ServerStore.NodeTag}, " +
                                $"{index.Name}/{index.Definition.Name}, " +
                                $"{index._indexingThread.Name} - {index._indexingThread.ManagedThreadId} . \n";
                    };
                }

                await store.ExecuteIndexAsync(new MyErrorRollingIndex());

                var runningNode = GetRunningNode(cluster, store);

                Indexes.WaitForIndexingErrors(store, indexNames: new[] { nameof(MyRollingIndex) }, nodeTag: runningNode);

                // let's try to fix it
                await store.ExecuteIndexAsync(new MyRollingIndex());

                var res = WaitForValue(() => count, 3);

                Assert.True(res == 3, info);

                await AssertWaitForValueAsync(() => Task.FromResult(count), 3L);

                await VerifyHistory(cluster, store);
            }
        }

        [Fact]
        public async Task RollingIndexDeployedSwapNow()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStoreForRollingIndexes(new Options
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
                    indexStore.ForTestingPurposesOnly().BeforeRollingIndexStart = index => mre.Wait(index.IndexingProcessCancellationToken);
                    indexStore.ForTestingPurposesOnly().OnRollingIndexFinished = index =>
                    {
                        dic.AddOrUpdate(index.Name, 1, (_, val) => val + 1);
                    };
                }

                await store.ExecuteIndexAsync(new MyRollingIndex());
                await store.ExecuteIndexAsync(new MyEditedRollingIndex());

                var name = nameof(MyRollingIndex);
                var replacementName = Constants.Documents.Indexing.SideBySideIndexNamePrefix + name;

                var runningNodeTag = GetRunningNode(cluster, store);
                var runningNode = cluster.Nodes.Single(n => n.ServerStore.NodeTag == runningNodeTag);
                var runningDb = await runningNode.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                await AssertWaitForNotNullAsync(() => Task.FromResult(runningDb.IndexStore.GetIndex(name)));
                await AssertWaitForNotNullAsync(() => Task.FromResult(runningDb.IndexStore.GetIndex(replacementName)));

                using (var token = new CancellationTokenSource(TimeSpan.FromMinutes(2)))
                {
                    runningDb.IndexStore.ReplaceIndexes(name, replacementName, token.Token);
                }

                mre.Set();

                WaitForIndexingInTheCluster(store, store.Database);

                await AssertWaitForValueAsync(() => Task.FromResult(dic.Keys.Count), 1);
                await AssertWaitForGreaterThanAsync(() => Task.FromResult(dic[nameof(MyRollingIndex)]), 2);

                await VerifyHistory(cluster, store);
            }
        }

        [Fact]
        public async Task RollingIndexDisableEnable()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStoreForRollingIndexes(new Options
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
                    indexStore.ForTestingPurposesOnly().OnRollingIndexStart = index => mre.Wait(index.IndexingProcessCancellationToken);
                    indexStore.ForTestingPurposesOnly().OnRollingIndexFinished = index =>
                    {
                        dic.AddOrUpdate(index.Name, 1, (_, val) => val + 1);
                    };
                }

                await store.ExecuteIndexAsync(new MyRollingIndex());

                var runningNodeTag = GetRunningNode(cluster, store);
                var otherNode = cluster.Nodes.First(n => n.ServerStore.NodeTag != runningNodeTag);
                var otherDb = await otherNode.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                var rollingIndex = await AssertWaitForNotNullAsync(() => Task.FromResult(otherDb.IndexStore.GetIndex(nameof(MyRollingIndex))));
                rollingIndex.Disable();
                rollingIndex.Enable();

                mre.Set();

                WaitForIndexingInTheCluster(store, store.Database);

                await AssertWaitForValueAsync(() => Task.FromResult(dic.Keys.Count), 1);
                await AssertWaitForGreaterThanAsync(() => Task.FromResult(dic[nameof(MyRollingIndex)]), 2);

                await VerifyHistory(cluster, store);
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

        private async Task VerifyHistory((List<RavenServer> Nodes, RavenServer Leader) cluster, DocumentStore store)
        {

            await AssertWaitForValueAsync(() =>
            {
                using (cluster.Leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var record = cluster.Leader.ServerStore.Cluster.ReadDatabase(ctx, store.Database);
                    var history = record.IndexesHistory;
                    var deployment = history[nameof(MyRollingIndex)][0].RollingDeployment;
                    Assert.True(deployment.All(x => x.Value.State == RollingIndexState.Done));

                    return Task.FromResult(deployment.Count);
                }
            }, 3);

            using (cluster.Leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var record = cluster.Leader.ServerStore.Cluster.ReadDatabase(ctx, store.Database);
                if (record.RollingIndexes?.ContainsKey(nameof(MyRollingIndex)) == true)
                    Assert.True(false, "RollingIndexes shouldn't contain 'MyRollingIndex'");
            }

        }

        private static string GetRunningNode((List<RavenServer> Nodes, RavenServer Leader) cluster, DocumentStore store)
        {
            using (cluster.Leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var record = cluster.Leader.ServerStore.Cluster.ReadDatabase(ctx, store.Database);
                if (record.RollingIndexes.TryGetValue(nameof(MyRollingIndex), out var rolling))
                {
                    var running = rolling.ActiveDeployments.Single(x => x.Value.State == RollingIndexState.Running);
                    return running.Key;
                }

                Assert.True(false, "RollingIndexes should contain 'MyRollingIndex'");
            }

            return null;
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

        private class MyErrorRollingIndex : AbstractIndexCreationTask<Order>
        {
            public MyErrorRollingIndex()
            {
                Map = orders => from order in orders
                                select new
                                {
                                    Name = order.Company,
                                    // ReSharper disable once IntDivisionByZero
                                    Sum = order.Lines.Sum(x => x.Quantity) / 0
                                };

                DeploymentMode = IndexDeploymentMode.Rolling;
            }

            public override string IndexName => nameof(MyRollingIndex);
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

        private class MyEditedRollingIndex2 : AbstractIndexCreationTask<Order>
        {
            public MyEditedRollingIndex2()
            {
                Map = orders => from order in orders
                                select new
                                {
                                    order.Company,
                                    order.Employee,
                                    order.ShipTo
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
    }
}
