using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
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

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (var cdeArray = new CountdownsArray(subscriptionsChainSize, SubscriptionsCount))
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = dBGroupSize,
                ModifyDocumentStore = s => s.Conventions.ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior.RoundRobin
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), cts.Token);
                    await session.SaveChangesAsync(cts.Token);
                }
                var databaseName = store.Database;

                var workerTasks = new List<Task>();
                for (var i = 0; i < SubscriptionsCount; i++)
                {
                    await GenerateWaitingSubscriptions(cdeArray.GetArray(), store, i, workerTasks, cts.Token);
                }

                _ = Task.Run(async () => await ContinuouslyGenerateDocs(DocsBatchSize, store, cts.Token), cts.Token);

                var dbNodesCountToToggle = Math.Max(Math.Min(dBGroupSize - 1, dBGroupSize / 2 + 1), 1);
                var nodesToToggle = store.GetRequestExecutor().TopologyNodes.Select(x => x.ClusterTag).Take(dbNodesCountToToggle).ToList();

                _toggled = true;
                foreach (var node in nodesToToggle)
                {
                    cts.Token.ThrowIfCancellationRequested();

                    var nodeIndex = cluster.Nodes.FindIndex(x => x.ServerStore.NodeTag == node);
                    await ToggleClusterNodeOnAndOffAndWaitForRehab(databaseName, cluster, nodeIndex, shouldTrapRevivedNodesIntoCandidate, cts.Token);
                }
                _toggled = false;

                Assert.All(cdeArray.GetArray(), cde => Assert.True(SubscriptionsCount == cde.CurrentCount, PrintTestInfo(nodesToToggle, cluster)));

                foreach (var cde in cdeArray.GetArray())
                {
                    await KeepDroppingSubscriptionsAndWaitingForCDE(databaseName, SubscriptionsCount, cluster, cde, cts.Token);
                }

                foreach (var curNode in cluster.Nodes)
                {
                    await AssertNoSubscriptionLeftAlive(databaseName, SubscriptionsCount, curNode, cts.Token);
                }

                await Task.WhenAll(workerTasks);
            }
        }

        private string PrintTestInfo(List<string> nodesToToggle, (List<Raven.Server.RavenServer> Nodes, Raven.Server.RavenServer Leader) cluster)
        {
            var sb = new StringBuilder();
            sb.AppendLine($"Leader: {cluster.Leader.ServerStore.NodeTag}");
            sb.AppendLine($"ToggledNodes: {string.Join(", ", nodesToToggle)}");
            sb.AppendLine("Exceptions on signal:");
            foreach ((SubscriptionWorker<dynamic> subscriptionWorker, Exception exception) in _testInfo)
            {
                AddInfo(sb, subscriptionWorker, exception);
            }

            sb.AppendLine();
            sb.AppendLine("Exceptions on retry connection:");

            foreach ((SubscriptionWorker<dynamic> subscriptionWorker, Exception exception) in _testInfoOnRetry)
            {
                AddInfo(sb, subscriptionWorker, exception);
            }

            _testInfo.Clear();
            _testInfoOnRetry.Clear();
            return sb.ToString();

            static void AddInfo(StringBuilder stringBuilder, SubscriptionWorker<dynamic> subscriptionWorker, Exception exception)
            {
                stringBuilder.AppendLine(subscriptionWorker == null
                    ? $"subscriptionWorker is null"
                    : $"SubscriptionName: {subscriptionWorker.SubscriptionName}, CurrentNodeTag: {subscriptionWorker.CurrentNodeTag}");
                stringBuilder.AppendLine(exception == null ? "Exception: None" : $"Exception: {exception}");
                stringBuilder.AppendLine("-----------------------");
            }
        }

        [Fact]
        public async Task MakeSureAllNodesAreRoundRobined()
        {
            const int clusterSize = 5;
            var cluster = AsyncHelpers.RunSync(() => CreateRaftCluster(clusterSize, shouldRunInMemory: false));

            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = clusterSize,
                ModifyDocumentStore = s => s.Conventions.ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior.RoundRobin,
                DeleteDatabaseOnDispose = false
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }
                var databaseName = store.Database;

                var subsId = store.Subscriptions.Create<User>();
                using var subsWorker = store.Subscriptions.GetSubscriptionWorker<User>(new Raven.Client.Documents.Subscriptions.SubscriptionWorkerOptions(subsId)
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

                List<string> toggledNodes = new List<string>();
                var toggleCount = Math.Round(clusterSize * 0.51);
                for (int i = 0; i < toggleCount; i++)
                {
                    string responsibleNode = null;
                    await ActionWithLeader(async l =>
                    {
                        var documentDatabase = await l.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                        using (documentDatabase.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            responsibleNode = documentDatabase.SubscriptionStorage.GetResponsibleNode(context, subsId);
                        }
                    });
                    Assert.NotNull(responsibleNode);
                    toggledNodes.Add(responsibleNode);
                    var nodeIndex = cluster.Nodes.FindIndex(x => x.ServerStore.NodeTag == responsibleNode);
                    var node = cluster.Nodes[nodeIndex];

                    var res = await DisposeServerAndWaitForFinishOfDisposalAsync(node);
                    Assert.Equal(responsibleNode, res.NodeTag);
                    await Task.Delay(5000);
                }
                while (clusterSize != redirects.Count)
                {
                    await Task.Delay(1000);
                    Assert.True(sp.Elapsed < TimeSpan.FromMinutes(2), $"sp.Elapsed < TimeSpan.FromMinutes(1), redirects count : {redirects.Count}, leaderNodeTag: {cluster.Leader.ServerStore.NodeTag}, missing: {string.Join(", ", cluster.Nodes.Select(x => x.ServerStore.NodeTag).Except(redirects))}, offline: {string.Join(", ", toggledNodes)}");
                }

                Assert.Equal(clusterSize, redirects.Count);
            }
        }

        [Fact]
        public async Task SubscriptionShouldReconnectOnExceptionInTcpListener()
        {
            using var server = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false,
                RegisterForDisposal = false,
            });
            using (var store = GetDocumentStore(new Options
            {
                Server = server,
                ModifyDocumentStore = s => s.Conventions.ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior.RoundRobin,
            }))
            {
                var mre = new AsyncManualResetEvent();
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                var subsId = await store.Subscriptions.CreateAsync<User>();
                using var subsWorker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subsId)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(16)
                });
                subsWorker.OnSubscriptionConnectionRetry += ex =>
                {
                    Assert.NotNull(ex);
                    Assert.True(ex.GetType() == typeof(IOException) || ex.GetType() == typeof(EndOfStreamException));
                    mre.Set();
                };
                var task = subsWorker.Run(x => { });
                server.ForTestingPurposesOnly().ThrowExceptionInListenToNewTcpConnection = true;
                try
                {
                    Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(30)));
                }
                finally
                {
                    server.ForTestingPurposesOnly().ThrowExceptionInListenToNewTcpConnection = false;
                }
            }
        }

        private static async Task AssertNoSubscriptionLeftAlive(string dbName, int SubscriptionsCount, Raven.Server.RavenServer curNode, CancellationToken token)
        {
            if (false == curNode.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(dbName, out var resourceTask))
                return;
            var db = await resourceTask.WithCancellation(token);

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

        private async Task GenerateWaitingSubscriptions(CountdownEvent[] cdes, DocumentStore store, int index, List<Task> workerTasks, CancellationToken token)
        {
            var subsId = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
            {
                Query = "from Users",
                Name = "Subscription" + index
            }, token: token);
            foreach (var cde in cdes)
            {
                workerTasks.Add(GenerateSubscriptionThatSignalsToCDEUponCompletion(cde, store, subsId, token));
                await Task.Delay(1000, token);
            }
        }

        private List<(SubscriptionWorker<dynamic>, Exception)> _testInfo = new List<(SubscriptionWorker<dynamic>, Exception)>();
        private List<(SubscriptionWorker<dynamic>, Exception)> _testInfoOnRetry = new List<(SubscriptionWorker<dynamic>, Exception)>();

        private Task GenerateSubscriptionThatSignalsToCDEUponCompletion(CountdownEvent mainSubscribersCompletionCDE, DocumentStore store, string subsId, CancellationToken token)
        {
            var subsWorker = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(subsId)
            {
                Strategy = SubscriptionOpeningStrategy.WaitForFree
            });
            var t = subsWorker.Run(s => { }, token).ContinueWith(res =>
                 {
                     mainSubscribersCompletionCDE.Signal();
                     if (res.Exception == null)
                     {
                         _testInfo.Add((subsWorker, null));
                         return;
                     }
                     _testInfo.Add((subsWorker, res.Exception));
                     if (res.Exception != null && res.Exception is AggregateException agg && agg.InnerException is SubscriptionClosedException sce &&
                         sce.Message.Contains("Dropped by Test"))
                         return;
                     throw res.Exception;
                 });

            subsWorker.OnSubscriptionConnectionRetry += ex =>
            {
                if (ex is SubscriptionDoesNotBelongToNodeException == false)
                {
                    _testInfoOnRetry.Add((subsWorker, ex));
                }
            };
            return t;
        }

        private async Task ToggleClusterNodeOnAndOffAndWaitForRehab(string databaseName, (List<Raven.Server.RavenServer> Nodes, Raven.Server.RavenServer Leader) cluster, int index, bool shouldTrapRevivedNodesIntoCandidate, CancellationToken token)
        {
            var node = cluster.Nodes[index];

            node = await ToggleServer(node, shouldTrapRevivedNodesIntoCandidate);
            cluster.Nodes[index] = node;

            await Task.Delay(5000, token);
            node = await ToggleServer(node, shouldTrapRevivedNodesIntoCandidate);
            cluster.Nodes[index] = node;

            await WaitForRehab(databaseName, index, cluster);
        }

        private async Task ContinuouslyGenerateDocs(int DocsBatchSize, DocumentStore store, CancellationToken token)
        {
            while (false == store.WasDisposed)
            {
                token.ThrowIfCancellationRequested();

                if (_toggled)
                {
                    await Task.Delay(200, token);
                    continue;
                }

                await ContinuouslyGenerateDocsInternal(DocsBatchSize, store, token);
            }
        }

        internal static async Task ContinuouslyGenerateDocsInternal(int DocsBatchSize, DocumentStore store, CancellationToken token)
        {
            try
            {
                var ids = new List<string>();
                using (var session = store.OpenAsyncSession(new SessionOptions
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
                        await session.StoreAsync(entity, token);
                        ids.Add(session.Advanced.GetDocumentId(entity));
                    }
                    await session.SaveChangesAsync(token);
                }

                using (var session = store.OpenAsyncSession())
                {
                    for (var k = 0; k < DocsBatchSize; k++)
                    {
                        await session.StoreAsync(new User
                        {
                            Name = "Johnny" + k
                        }, token);
                    }
                    await session.SaveChangesAsync(token);
                }

                using (var session = store.OpenAsyncSession())
                {
                    for (var k = 0; k < DocsBatchSize; k++)
                    {
                        var user = await session.LoadAsync<User>(ids[k]);
                        user.Age++;
                    }
                    await session.SaveChangesAsync(token);
                }
                await Task.Delay(16, token);
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

        private static async Task KeepDroppingSubscriptionsAndWaitingForCDE(string databaseName, int SubscriptionsCount, (List<Raven.Server.RavenServer> Nodes, Raven.Server.RavenServer Leader) cluster, CountdownEvent mainSubscribersCDE, CancellationToken token)
        {
            var mainTcs = new TaskCompletionSource<bool>();

            var mainTI = ThreadPool.RegisterWaitForSingleObject(mainSubscribersCDE.WaitHandle, (x, timedOut) =>
            {
                if (!timedOut)
                    mainTcs.SetResult(true);
            }, null, 10000, true);

            for (int i = 0; i < 20; i++)
            {
                await DropSubscriptions(databaseName, SubscriptionsCount, cluster, token);

                if (await mainTcs.Task.WaitAsync(1000))
                    break;
            }

            mainTI.Unregister(mainSubscribersCDE.WaitHandle);
        }

        private async Task WaitForRehab(string dbName, int index, (List<Raven.Server.RavenServer> Nodes, Raven.Server.RavenServer Leader) cluster)
        {
            var toggled = cluster.Nodes[index].ServerStore.NodeTag;
            List<Exception> errors = new List<Exception>();
            for (var i = 0; i < cluster.Nodes.Count; i++)
            {
                var curNode = cluster.Nodes[i];
                var attempts = 20;
                List<string> rehabNodes;
                do
                {
                    using (curNode.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        try
                        {
                            rehabNodes = curNode.ServerStore.Cluster.ReadDatabaseTopology(context, dbName).Rehabs;
                        }
                        catch (Exception e)
                        {
                            errors.Add(e);
                            rehabNodes = new List<string> { "error" };
                        }
                    }
                    await Task.Delay(1000);
                }
                while (--attempts > 0 && rehabNodes.Count > 0);

                if ((attempts >= 0 && rehabNodes.Count == 0) == false)
                {
                    var sb = new StringBuilder();
                    (ClusterObserverLogEntry[] List, long Iteration) logs;
                    logs.List = null;
                    await ActionWithLeader((l) =>
                    {
                        logs = l.ServerStore.Observer.ReadDecisionsForDatabase();
                        return Task.CompletedTask;
                    });

                    sb.AppendLine("Cluster Observer Log Entries:\n-----------------------");
                    foreach (var log in logs.List)
                    {
                        sb.AppendLine(
                            $"{nameof(log.Date)}: {log.Date}\n{nameof(log.Database)}: {log.Database}\n{nameof(log.Iteration)}: {log.Iteration}\n{nameof(log.Message)}: {log.Message}\n-----------------------");
                    }

                    List<string> currentRehabNodes;
                    string currentException = null;
                    using (curNode.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        try
                        {
                            currentRehabNodes = curNode.ServerStore.Cluster.ReadDatabaseTopology(context, dbName).Rehabs;
                        }
                        catch (Exception e)
                        {
                            currentException = e.ToString();
                            currentRehabNodes = new List<string> { "another error" };
                        }
                    }

                    sb.AppendLine($"\nLast attempt:\n rehabsCount: {currentRehabNodes.Count}, rehabNodes: {string.Join(", ", currentRehabNodes)}, exception: {currentException}");

                    sb.AppendLine($"\nwaited for rehab for too long, current node: {cluster.Nodes[i].ServerStore.NodeTag}, toggled node: {toggled}, " +
                                  $"rehabsCount: {rehabNodes.Count}, rehabNodes: {string.Join(", ", rehabNodes)}, " +
                                  $"attempts: {attempts}, errors: {string.Join("\n", errors)}");
                    Assert.True(attempts >= 0 && rehabNodes.Count == 0, sb.ToString());
                }
            }
        }

        private static async Task DropSubscriptions(string databaseName, int SubscriptionsCount, (List<Raven.Server.RavenServer> Nodes, Raven.Server.RavenServer Leader) cluster, CancellationToken token)
        {
            foreach (var curNode in cluster.Nodes)
            {
                Raven.Server.Documents.DocumentDatabase db = null;
                try
                {
                    db = await curNode.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).WithCancellation(token);
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
                }, caller: $"{node.DebugTag}-{nameof(ToggleServer)}");

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
