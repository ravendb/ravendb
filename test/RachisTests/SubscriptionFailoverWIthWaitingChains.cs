using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace RachisTests
{
    public class SubscriptionFailoverWithWaitingChains : ClusterTestBase
    {
        private class CountdownsArray : IDisposable
        {
            private CountdownEvent[] _array;

            public CountdownsArray(int arraySize, int countdownCount)
            {
                _array = new CountdownEvent[arraySize];
                for (var i = 0; i < arraySize; i++)
                {
                    _array[i] = new CountdownEvent(countdownCount);
                }
            }

            public CountdownEvent[] GetArray()
            {
                return _array.ToArray();
            }

            public void Dispose()
            {
                foreach (var cde in _array)
                {
                    cde.Dispose();
                }
            }
        }

        public SubscriptionFailoverWithWaitingChains(ITestOutputHelper output) : base(output)
        {
        }

        private class TestParams : DataAttribute
        {
            public TestParams(int subscriptionsChainSize, int clusterSize, int dBGroupSize, bool shouldTrapRevivedNodesIntoCandidate)
            {
                SubscriptionsChainSize = subscriptionsChainSize;
                ClusterSize = clusterSize;
                DBGroupSize = dBGroupSize;
                ShouldTrapRevivedNodesIntoCandidate = shouldTrapRevivedNodesIntoCandidate;
            }

            public int SubscriptionsChainSize { get; }
            public int ClusterSize { get; }
            public int DBGroupSize { get; }
            public bool ShouldTrapRevivedNodesIntoCandidate { get; }

            public override IEnumerable<object[]> GetData(MethodInfo testMethod)
            {
                yield return new object[]{
                    SubscriptionsChainSize,
                    ClusterSize,
                    DBGroupSize,
                    ShouldTrapRevivedNodesIntoCandidate};
            }
        }

        private bool _toggled;

        [Theory(Skip = "Uprobable, intermediate state")]
        [TestParams(subscriptionsChainSize: 2, clusterSize: 5, dBGroupSize: 3, shouldTrapRevivedNodesIntoCandidate: true)]
        public async Task SkippedSubscriptionsShouldFailoverAndReturnToOriginalNodes(int subscriptionsChainSize, int clusterSize, int dBGroupSize, bool shouldMaintainElectionTimeout)
        {
            await SubscriptionsShouldFailoverCorrectrlyAndAllowThemselvesToBeTerminated(subscriptionsChainSize, clusterSize, dBGroupSize, shouldMaintainElectionTimeout);
        }

        [Theory]
        [TestParams(subscriptionsChainSize: 2, clusterSize: 3, dBGroupSize: 3, shouldTrapRevivedNodesIntoCandidate: false)]
        [TestParams(subscriptionsChainSize: 2, clusterSize: 5, dBGroupSize: 3, shouldTrapRevivedNodesIntoCandidate: false)]
        [TestParams(subscriptionsChainSize: 3, clusterSize: 5, dBGroupSize: 3, shouldTrapRevivedNodesIntoCandidate: false)]
        [TestParams(subscriptionsChainSize: 3, clusterSize: 5, dBGroupSize: 5, shouldTrapRevivedNodesIntoCandidate: false)]
        public async Task SubscriptionsShouldFailoverCorrectrlyAndAllowThemselvesToBeTerminated(int subscriptionsChainSize, int clusterSize, int dBGroupSize, bool shouldTrapRevivedNodesIntoCandidate)
        {
            const int SubscriptionsCount = 10;
            const int DocsBatchSize = 10;
            var cluster = await CreateRaftCluster(clusterSize, shouldRunInMemory: false);

            using (var cdeArray = new CountdownsArray(subscriptionsChainSize, SubscriptionsCount))
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = dBGroupSize,
                ModifyDocumentStore = s => s.Conventions.ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior.RoundRobin
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }
                var databaseName = store.Database;

                var workerTasks = new List<Task>();
                for (var i = 0; i < SubscriptionsCount; i++)
                {
                    await GenerateWaitingSubscriptions(cdeArray.GetArray(), store, i, workerTasks);
                }

                var task = Task.Run(async () =>
                {
                    await ContinuouslyGenerateDocs(DocsBatchSize, store);
                });

                var dbNodesCountToToggle = Math.Max(Math.Min(dBGroupSize - 1, dBGroupSize / 2 + 1), 1);
                var nodesToToggle = store.GetRequestExecutor().TopologyNodes.Select(x => x.ClusterTag).Take(dbNodesCountToToggle);

                foreach (var node in nodesToToggle)
                {
                    var nodeIndex = cluster.Nodes.FindIndex(x => x.ServerStore.NodeTag == node);
                    await ToggleClusterNodeOnAndOffAndWaitForRehab(databaseName, cluster, nodeIndex, shouldTrapRevivedNodesIntoCandidate);
                }

                Assert.All(cdeArray.GetArray(), cde => Assert.Equal(SubscriptionsCount, cde.CurrentCount));

                foreach (var cde in cdeArray.GetArray())
                {
                    await KeepDroppingSubscriptionsAndWaitingForCDE(databaseName, SubscriptionsCount, cluster, cde);
                }

                foreach (var curNode in cluster.Nodes)
                {
                    await AssertNoSubscriptionLeftAlive(databaseName, SubscriptionsCount, curNode);
                }

                await Task.WhenAll(workerTasks);
            }
        }

        [Theory]
        [InlineData(5)]
        public async Task MakeSureAllNodesAreRoundRobined(int clusterSize)
        {
            var cluster = AsyncHelpers.RunSync(() => CreateRaftCluster(clusterSize, shouldRunInMemory: false));

            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = clusterSize,
                ModifyDocumentStore = s => s.Conventions.ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior.RoundRobin
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }
                var databaseName = store.Database;

                var subsId = store.Subscriptions.Create<User>();
                var subsWorker = store.Subscriptions.GetSubscriptionWorker<User>(new Raven.Client.Documents.Subscriptions.SubscriptionWorkerOptions(subsId)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(16)
                }
                );

                HashSet<string> redirects = new HashSet<string>();

                subsWorker.OnSubscriptionConnectionRetry += ex =>
                {
                    redirects.Add(subsWorker.CurrentNodeTag);
                };
                var task = subsWorker.Run(x => { });

                var sp = Stopwatch.StartNew();

                var nodes = store.GetRequestExecutor().TopologyNodes.Select(x => x.ClusterTag).ToList();
                Assert.Equal(clusterSize, cluster.Nodes.Count);
                Assert.Equal(clusterSize, nodes.Count);

                foreach (var node in nodes)
                {
                    var nodeIndex = cluster.Nodes.FindIndex(x => x.ServerStore.NodeTag == node);
                    await ToggleClusterNodeOnAndOffAndWaitForRehab(databaseName, cluster, nodeIndex, shouldTrapRevivedNodesIntoCandidate: true);

                }
                while (clusterSize != redirects.Count)
                {
                    await ActionWithLeader((l) =>
                    {
                        l.ServerStore.Engine.CurrentLeader?.StepDown();
                        return Task.CompletedTask;
                    });

                    Thread.Sleep(16);
                    Assert.True(sp.Elapsed < TimeSpan.FromMinutes(5), $"sp.Elapsed < TimeSpan.FromMinutes(5), redirects count : {redirects.Count}, leaderNodeTag: {cluster.Leader.ServerStore.NodeTag}, missing: {string.Join(", ", cluster.Nodes.Select(x => x.ServerStore.NodeTag).Except(redirects))}");
                }

                Assert.Equal(clusterSize, redirects.Count);
            }
        }

        private static async Task AssertNoSubscriptionLeftAlive(string dbName, int SubscriptionsCount, Raven.Server.RavenServer curNode)
        {
            if (false == curNode.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(dbName, out var resourceTask))
                return;
            var db = await resourceTask;

            for (var k = 0; k < SubscriptionsCount; k++)
            {
                using (curNode.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var subscription = db
                        .SubscriptionStorage
                        .GetRunningSubscription(context, null, "Subscription" + k, false);

                    if (subscription != null)
                        Assert.True(false, "no subscriptions should be alive at this point");
                }
            }
        }

        private static async Task GenerateWaitingSubscriptions(CountdownEvent[] cdes, DocumentStore store, int index, List<Task> workerTasks)
        {
            var subsId = store.Subscriptions.Create(new Raven.Client.Documents.Subscriptions.SubscriptionCreationOptions
            {
                Query = "from Users",
                Name = "Subscription" + index
            });
            foreach (var cde in cdes)
            {
                workerTasks.Add(GenerateSubscriptionThatSignalsToCDEUponCompletion(cde, store, subsId));
                await Task.Delay(1000);
            }
        }

        private static Task GenerateSubscriptionThatSignalsToCDEUponCompletion(CountdownEvent mainSubscribersCompletionCDE, DocumentStore store, string subsId)
        {
            return store.Subscriptions.GetSubscriptionWorker(new Raven.Client.Documents.Subscriptions.SubscriptionWorkerOptions(subsId)
            {
                Strategy = Raven.Client.Documents.Subscriptions.SubscriptionOpeningStrategy.WaitForFree
            })
                        .Run(s => { })
                        .ContinueWith(res =>
                        {
                            mainSubscribersCompletionCDE.Signal();
                            if (res.Exception == null)
                                return;
                            if (res.Exception != null && res.Exception is AggregateException agg && agg.InnerException is SubscriptionClosedException sce && sce.Message.Contains("Dropped by Test"))
                                return;
                            throw res.Exception;
                        });
        }

        private async Task ToggleClusterNodeOnAndOffAndWaitForRehab(string databaseName, (List<Raven.Server.RavenServer> Nodes, Raven.Server.RavenServer Leader) cluster, int index, bool shouldTrapRevivedNodesIntoCandidate)
        {
            var node = cluster.Nodes[index];

            node = await ToggleServer(node, shouldTrapRevivedNodesIntoCandidate);
            cluster.Nodes[index] = node;

            await Task.Delay(5000);
            node = await ToggleServer(node, shouldTrapRevivedNodesIntoCandidate);
            cluster.Nodes[index] = node;

            _toggled = true;
            await WaitForRehab(databaseName, index, cluster);
            _toggled = false;
        }

        private async Task ContinuouslyGenerateDocs(int DocsBatchSize, DocumentStore store)
        {
            while (false == store.WasDisposed)
            {
                if (_toggled)
                {
                    await Task.Delay(200);
                    continue;
                }

                try
                {
                    var ids = new List<string>();
                    using (var session = store.OpenSession(new SessionOptions
                    {
                        TransactionMode = TransactionMode.ClusterWide
                    }))
                    {
                        for (var k = 0; k < DocsBatchSize; k++)
                        {
                            User entity = new User
                            {
                                Name = "ClusteredJohnny" + k
                            };
                            session.Store(entity);
                            ids.Add(session.Advanced.GetDocumentId(entity));
                        }
                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession())
                    {
                        for (var k = 0; k < DocsBatchSize; k++)
                        {
                            session.Store(new User
                            {
                                Name = "Johnny" + k
                            });
                        }
                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession())
                    {
                        for (var k = 0; k < DocsBatchSize; k++)
                        {
                            var user = session.Load<User>(ids[k]);
                            user.Age++;
                        }
                        session.SaveChanges();
                    }
                    await Task.Delay(16);
                }
                catch (AllTopologyNodesDownException)
                {
                }
                catch (DatabaseDisabledException)
                {
                }
                catch (DatabaseDoesNotExistException)
                {
                }
                catch (RavenException)
                {
                }
            }
        }

        private static async Task KeepDroppingSubscriptionsAndWaitingForCDE(string databaseName, int SubscriptionsCount, (List<Raven.Server.RavenServer> Nodes, Raven.Server.RavenServer Leader) cluster, CountdownEvent mainSubscribersCDE)
        {
            var mainTcs = new TaskCompletionSource<bool>();

            var mainTI = ThreadPool.RegisterWaitForSingleObject(mainSubscribersCDE.WaitHandle, (x, timedOut) =>
            {
                if (!timedOut)
                    mainTcs.SetResult(true);
            }, null, 10000, true);

            for (int i = 0; i < 20; i++)
            {
                await DropSubscriptions(databaseName, SubscriptionsCount, cluster);

                if (await mainTcs.Task.WaitAsync(1000))
                    break;
            }

            mainTI.Unregister(mainSubscribersCDE.WaitHandle);
        }

        private static async Task WaitForRehab(string dbName, int index, (List<Raven.Server.RavenServer> Nodes, Raven.Server.RavenServer Leader) cluster)
        {
            var toggled = cluster.Nodes[index].ServerStore.NodeTag;
            for (var i = 0; i < cluster.Nodes.Count; i++)
            {
                var curNode = cluster.Nodes[i];
                var rehabCount = 0;
                var attempts = 20;
                do
                {
                    using (curNode.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        try
                        {
                            rehabCount = curNode.ServerStore.Cluster.ReadDatabaseTopology(context, dbName).Rehabs.Count;
                        }
                        catch (Exception)
                        {
                            await Task.Delay(1000);
                            rehabCount = 1;
                            continue;
                        }
                        await Task.Delay(1000);
                    }
                }
                while (--attempts > 0 && rehabCount > 0);
                Assert.True(attempts >= 0 && rehabCount == 0, $"waited for rehab for too long, current node: {cluster.Nodes[i].ServerStore.NodeTag}, toggled node: {toggled}, rehabs: {rehabCount}, attempts: {attempts}");
            }
        }

        private static async Task DropSubscriptions(string databaseName, int SubscriptionsCount, (List<Raven.Server.RavenServer> Nodes, Raven.Server.RavenServer Leader) cluster)
        {
            foreach (var curNode in cluster.Nodes)
            {
                Raven.Server.Documents.DocumentDatabase db = null;
                try
                {
                    db = await curNode.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                }
                catch (DatabaseNotRelevantException)
                {
                    continue;
                }

                for (var k = 0; k < SubscriptionsCount; k++)
                {
                    using (curNode.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var subscription = db
                            .SubscriptionStorage
                            .GetRunningSubscription(context, null, "Subscription" + k, false);

                        if (subscription == null)
                            continue;
                        db.SubscriptionStorage.DropSubscriptionConnection(subscription.SubscriptionId,
                            new SubscriptionClosedException("Dropped by Test"));
                    }
                }
            }
        }

        private async Task<Raven.Server.RavenServer> ToggleServer(Raven.Server.RavenServer node, bool shouldTrapRevivedNodesIntoCandidate)
        {
            if (node.Disposed)
            {
                var settings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "1",
                    [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
                    [RavenConfiguration.GetKey(x => x.Cluster.AddReplicaTimeout)] = "1",
                    [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = node.WebUrl
                };

                var dataDirectory = node.Configuration.Core.DataDirectory.FullPath;

                // if we want to make sure that the revived node will be trapped in candidate node, we should make sure that the election timeout value is different from the
                // rest of the node (note that this is a configuration value, therefore we need to define it in "settings" and nowhere else)
                if (shouldTrapRevivedNodesIntoCandidate == false)
                    settings[RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = node.Configuration.Cluster.ElectionTimeout.AsTimeSpan.TotalMilliseconds.ToString();

                node = base.GetNewServer(new ServerCreationOptions()
                {
                    DeletePrevious = false,
                    RunInMemory = false,
                    CustomSettings = settings,
                    DataDirectory = dataDirectory
                });

                Assert.True(node.ServerStore.Engine.CurrentState != RachisState.Passive, "node.ServerStore.Engine.CurrentState != RachisState.Passive");
            }
            else
            {
                var result = await DisposeServerAndWaitForFinishOfDisposalAsync(node);
            }

            return node;
        }
    }
}
