using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RachisTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Collections;
using Sparrow.Platform;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace StressTests.Rachis
{
    public class SubscriptionFailoverWithWaitingChainsStress : ClusterTestBase
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

        public SubscriptionFailoverWithWaitingChainsStress(ITestOutputHelper output) : base(output)
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
                ModifyDocumentStore = s => s.Conventions.ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior.RoundRobin,
                RunInMemory = false
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
                    await GenerateWaitingSubscriptions(cdeArray.GetArray(), store, i, workerTasks, cluster.Nodes, cts.Token);
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
                    await KeepDroppingSubscriptionsAndWaitingForCDE(databaseName, SubscriptionsCount, cluster, cde, dBGroupSize, cts.Token);
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

        private async Task GenerateWaitingSubscriptions(CountdownEvent[] cdes, DocumentStore store, int index, List<Task> workerTasks, List<RavenServer> nodes, CancellationToken token)
        {
            var subsId = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
            {
                Query = "from Users",
                Name = "Subscription" + index
            }, token: token);

            var subscription = await GetSubscription(subsId, store.Database, nodes, token);
            Assert.NotNull(subscription);

            await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(subscription.SubscriptionId, nodes);

            foreach (var cde in cdes)
            {
                workerTasks.Add(GenerateSubscriptionThatSignalsToCDEUponCompletion(cde, store, subsId, token));
                await Task.Delay(1000, token);
            }
        }

        private ConcurrentSet<(SubscriptionWorker<dynamic>, Exception)> _testInfo = new ConcurrentSet<(SubscriptionWorker<dynamic>, Exception)>();
        private ConcurrentSet<(SubscriptionWorker<dynamic>, Exception)> _testInfoOnRetry = new ConcurrentSet<(SubscriptionWorker<dynamic>, Exception)>();

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

            node = await SubscriptionsFailover.ToggleServer(node, shouldTrapRevivedNodesIntoCandidate, base.GetNewServer);
            cluster.Nodes[index] = node;

            await Task.Delay(5000, token);
            node = await SubscriptionsFailover.ToggleServer(node, shouldTrapRevivedNodesIntoCandidate, base.GetNewServer);
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
                        var user = await session.LoadAsync<User>(ids[k], token);
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

        private static async Task KeepDroppingSubscriptionsAndWaitingForCDE(string databaseName, int SubscriptionsCount,
            (List<RavenServer> Nodes, RavenServer Leader) cluster, CountdownEvent mainSubscribersCDE, int dBGroupSize, CancellationToken token)
        {
            Dictionary<string, List<string>> res = null;
            var dropResult = await WaitForValueAsync(async () =>
            {
                res = await DropSubscriptions(databaseName, SubscriptionsCount, cluster, dBGroupSize, token);

                if (WaitHandle.WaitAny(new[] { mainSubscribersCDE.WaitHandle }, 1000) == WaitHandle.WaitTimeout)
                {
                    return false;
                }

                return true;
            }, true, timeout: PlatformDetails.Is32Bits ? 5 * 60_000 : 60_000);

            var msg = "Dropped subscriptions, didn't signal CDE.";
            Assert.True(dropResult, res == null ? msg : $"{msg}{Environment.NewLine}{string.Join(Environment.NewLine, res.Select(x=> $"{x.Key}: {string.Join(",", x.Value)}"))}");
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

        private static async Task<Dictionary<string, List<string>>> DropSubscriptions(string databaseName, int SubscriptionsCount, (List<RavenServer> Nodes, RavenServer Leader) cluster, int dBGroupSize,
            CancellationToken token)
        {
            var res = new Dictionary<string, List<string>>();

            foreach (var curNode in cluster.Nodes)
            {
                res.Add(curNode.ServerStore.NodeTag, new List<string>());
                DocumentDatabase db;
                try
                {
                    db = await curNode.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).WithCancellation(token);
                }
                catch (DatabaseNotRelevantException)
                {
                    if (dBGroupSize == cluster.Nodes.Count)
                        throw;

                    continue;
                }

                for (var k = 0; k < SubscriptionsCount; k++)
                {
                    using (curNode.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var name = $"Subscription{k}";
                        var subscription = db
                            .SubscriptionStorage
                            .GetRunningSubscription(context, null, name, false);

                        if (subscription == null)
                        {
                            continue;
                        }

                        res[curNode.ServerStore.NodeTag].Add(name);
                        db.SubscriptionStorage.DropSubscriptionConnections(subscription.SubscriptionId,
                            new SubscriptionClosedException("Dropped by Test"));
                    }
                }
            }

            return res;
        }

        internal static async Task<SubscriptionStorage.SubscriptionGeneralDataAndStats> GetSubscription(string name, string database, List<RavenServer> nodes, CancellationToken token = default)
        {
            foreach (var curNode in nodes)
            {
                DocumentDatabase db;
                try
                {
                    db = await curNode.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database).WithCancellation(token);
                }
                catch (DatabaseNotRelevantException)
                {
                    continue;
                }

                using (curNode.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    SubscriptionStorage.SubscriptionGeneralDataAndStats subscription = null;
                    try
                    {
                        subscription = db
                            .SubscriptionStorage
                            .GetSubscription(context, id: null, name, history: false);
                    }
                    catch (SubscriptionDoesNotExistException)
                    {
                        // expected
                    }

                    if (subscription == null)
                        continue;

                    return subscription;
                }
            }

            return null;
        }
    }
}
