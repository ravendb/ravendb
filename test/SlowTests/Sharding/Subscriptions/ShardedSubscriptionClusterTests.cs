using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Subscriptions
{
    public class ShardedSubscriptionClusterTests : ClusterTestBase
    {
        public ShardedSubscriptionClusterTests(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromSeconds(60);

        [Fact]
        public async Task CanRunShardedSubscriptionInCluster()
        {
            var db = GetDatabaseName();
            var (nodes, leader) = await CreateRaftCluster(3);
            await ShardingCluster.CreateShardedDatabaseInCluster(db, 2, (nodes, leader));
            using (var store = new DocumentStore { Database = db, Urls = new[] { leader.WebUrl } }.Initialize())
            {
                var id = store.Subscriptions.Create<User>();
                var subscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                Assert.Equal(1, subscriptions.Count);

                var names = new List<string>
                {
                    "EGOR", "egor", "EGR"
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = names[0] }, Guid.NewGuid().ToString());
                    session.Store(new User() { Name = names[1] }, Guid.NewGuid().ToString());
                    session.Store(new User() { Name = names[2] }, Guid.NewGuid().ToString());
                    session.SaveChanges();
                }

                var mre = new AsyncManualResetEvent();
                using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)))
                {
                    var c = 0;
                    var t = subscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            names.Remove(item.Result.Name);
                            if (++c == 3)
                            {
                                mre.Set();
                            }
                        }
                    });

                    Assert.True(await mre.WaitAsync(_reasonableWaitTime));
                    Assert.Empty(names);
                }
            }
        }

        [Fact]
        public async Task SubscriptionShouldTryConnectWithTimeoutIfShardUnavailable()
        {
            int rf = 1;
            int clusterSize = 3;
            var (nodes, leader) = await CreateRaftCluster(clusterSize, shouldRunInMemory: false);

            var options = Sharding.GetOptionsForCluster(leader, shards: 3, shardReplicationFactor: 1, orchestratorReplicationFactor: clusterSize);
            options.RunInMemory = false;

            using (var store = Sharding.GetDocumentStore(options))
            {
                var id = store.Subscriptions.Create<User>();
                var subscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                Assert.Equal(1, subscriptions.Count);

                var servers = await ShardingCluster.GetShardsDocumentDatabaseInstancesFor(store, nodes);
                while (Sharding.AllShardHaveDocs(servers) == false)
                {
                    using (var session = store.OpenSession())
                    {
                        var guid = Guid.NewGuid().ToString();
                        session.Store(new User { Name = $"EGOR_" }, guid);
                        guid = Guid.NewGuid().ToString();
                        session.Store(new User { Name = $"egor_" }, guid);
                        guid = Guid.NewGuid().ToString();
                        session.Store(new User { Name = $"EGR_" }, guid);
                        session.SaveChanges();
                    }
                }

                // Wait for documents to replicate
                Assert.True(await ShardingCluster.WaitForShardedChangeVectorInClusterAsync(nodes, store.Database, rf));

                var disposed = new List<string>();
                var hashset = new HashSet<string>();
                var tagsToDispose = new HashSet<string>();
                var nodesWithIds = new Dictionary<string, Dictionary<string, List<string>>>();
                foreach (var kvp in servers)
                {
                    var internalDic = new Dictionary<string, List<string>>();
                    foreach (var documentDatabase in kvp.Value)
                    {
                        internalDic[documentDatabase.Name] = new List<string>();
                        using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        {
                            context.OpenReadTransaction();
                            var ids = documentDatabase.DocumentsStorage.GetAllIds(context).Select(x => x.ToString(CultureInfo.InvariantCulture)).ToList();
                            if (ids.Count > 0)
                            {
                                hashset.Add(documentDatabase.Name);
                                tagsToDispose.Add(kvp.Key);
                            }
                            internalDic[documentDatabase.Name].AddRange(ids);
                        }
                    }
                    nodesWithIds.Add(kvp.Key, internalDic);
                }

                Assert.True(tagsToDispose.Count == 3, $"{tagsToDispose.Count} != 3");

                var c1 = rf;
                while (c1-- != 0)
                {
                    var i = new Random().Next(tagsToDispose.Count);
                    var tag = tagsToDispose.ElementAt(i);

                    var node = nodes.FirstOrDefault(x => x.ServerStore.NodeTag == tag);
                    Assert.NotNull(node);
                    var disposedNode = await DisposeServerAndWaitForFinishOfDisposalAsync(node);
                    disposed.Add(tag);

                    Assert.True(tagsToDispose.Remove(tag), "tagsToDispose.Remove(tag)");
                }

                var expectedIds = new List<string>();
                var expectedIds2 = new List<string>();

                foreach (var kvp in nodesWithIds)
                {
                    if (disposed.Contains(kvp.Key))
                    {
                        foreach (var shards in kvp.Value)
                        {
                            expectedIds2.AddRange(shards.Value);
                        }
                        continue;
                    }

                    foreach (var shards in kvp.Value)
                    {
                        expectedIds.AddRange(shards.Value);
                    }
                }

                Assert.NotEmpty(expectedIds);
                Assert.NotEmpty(expectedIds2);

                var mre = new AsyncManualResetEvent();
                var mre2 = new AsyncManualResetEvent();
                List<string> results = new List<string>();
                using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)))
                {
                    var c = 0;
                    var t = subscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            c++;
                            results.Add(item.Id);
                        }

                        if (c == expectedIds.Count)
                            mre.Set();

                        if (c == expectedIds.Count + expectedIds2.Count)
                            mre2.Set();
                    });

                    Assert.True(await mre.WaitAsync(_reasonableWaitTime),$"error: {t.Exception}");

                    Assert.All(expectedIds, s => Assert.Contains(s, results));
                    results.Clear();

                    var dispsoedNode = nodes.FirstOrDefault(x => x.ServerStore.NodeTag == disposed.First());
                    Assert.NotNull(dispsoedNode);

                    await ReviveNodeAsync((dispsoedNode.Configuration.Core.DataDirectory.FullPath, dispsoedNode.WebUrl, dispsoedNode.ServerStore.NodeTag));

                    // disposed node should reconnect and send docs
                    Assert.True(await mre2.WaitAsync(_reasonableWaitTime), $"error: {t.Exception}");
                    Assert.All(expectedIds2, s => Assert.Contains(s, results));
                }
            }
        }

        [Fact]
        public async Task SubscriptionShouldFailoverIfNodeIsDownButShardIsAvailable()
        {
            int rf = 2;
            var db = GetDatabaseName();
            var (nodes, leader) = await CreateRaftCluster(3, shouldRunInMemory: false);
            await ShardingCluster.CreateShardedDatabaseInCluster(db, rf, (nodes, leader));

            using (var store = new DocumentStore { Database = db, Urls = new[] { leader.WebUrl } }.Initialize())
            {
                var id = store.Subscriptions.Create<User>();
                var subscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                Assert.Equal(1, subscriptions.Count);

                using (var session = store.OpenSession())
                {
                    var guid = Guid.NewGuid().ToString();
                    session.Store(new User { Name = "EGOR" }, guid);
                    guid = Guid.NewGuid().ToString();
                    session.Store(new User { Name = "egor" }, guid);
                    guid = Guid.NewGuid().ToString();
                    session.Store(new User { Name = "EGR" }, guid);
                    session.SaveChanges();
                }

                // Wait for documents to replicate
                Assert.True(ShardingCluster.WaitForShardedChangeVectorInCluster(nodes, store.Database, rf));

                var servers = await ShardingCluster.GetShardsDocumentDatabaseInstancesFor(store, nodes);

                var nodesWithIds = new Dictionary<string, List<string>>();
                foreach (var kvp in servers)
                {
                    nodesWithIds.Add(kvp.Key, new List<string>());
                    foreach (var documentDatabase in kvp.Value)
                    {
                        using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        {
                            context.OpenReadTransaction();
                            var ids = documentDatabase.DocumentsStorage.GetAllIds(context).Select(x => x.ToString(CultureInfo.InvariantCulture)).ToList();
                            nodesWithIds[kvp.Key].AddRange(ids);
                        }
                    }
                }

                var list = new List<RavenServer>(nodes);
                var c1 = rf;
                while (--c1 != 0)
                {
                    var i = new Random().Next(list.Count);
                    await DisposeServerAndWaitForFinishOfDisposalAsync(list[i]);
                    nodesWithIds.Remove(list[i].ServerStore.NodeTag);
                }

                int docs = 3;
                var mre = new AsyncManualResetEvent();
                List<string> results = new List<string>();
                using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)))
                {
                    var c = 0;
                    var t = subscription.Run(x =>
                    {

                        foreach (var item in x.Items)
                        {
                            c++;
                            results.Add(item.Id);
                        }

                        if (c == docs)
                            mre.Set();
                    });

                    Assert.True(await mre.WaitAsync(_reasonableWaitTime));

                    foreach (var item in nodesWithIds.SelectMany(kvp => kvp.Value))
                    {
                        results.Remove(item);
                    }

                    Assert.Empty(results);
                }
            }
        }
    }
}
