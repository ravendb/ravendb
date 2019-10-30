using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests
{
    public class SubscriptionFailoverWithWaitingChains : ClusterTestBase
    {

        public class CountdownsArray : IDisposable
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

        [Theory]        
        [InlineData(/*subscriptionsChainSize:*/2,/*clusterSize:*/3,/*dBGroupSize:*/3, /*shouldMaintainElectionTimeout:*/false)]        
        [InlineData(/*subscriptionsChainSize:*/2,/*clusterSize:*/3,/*dBGroupSize:*/3, /*shouldMaintainElectionTimeout:*/true)]
        [InlineData(/*subscriptionsChainSize:*/2,/*clusterSize:*/5,/*dBGroupSize:*/3, /*shouldMaintainElectionTimeout:*/false)]
        [InlineData(/*subscriptionsChainSize:*/2,/*clusterSize:*/5,/*dBGroupSize:*/3, /*shouldMaintainElectionTimeout:*/true)]
        //[InlineData(/*subscriptionsChainSize:*/3,/*clusterSize:*/5,/*dBGroupSize:*/3, /*shouldMaintainElectionTimeout:*/false)]
        public async Task SubscriptionsShouldFailoverAndReturnToOriginalNodes(int subscriptionsChainSize, int clusterSize ,int dBGroupSize, bool shouldMaintainElectionTimeout)
        {
            const int SubscriptionsCount = 20;
            const int DocsBatchSize = 10;
            var cluster = await CreateRaftCluster(clusterSize, shouldRunInMemory: false);
            
            using (var cdeArray = new CountdownsArray(subscriptionsChainSize, SubscriptionsCount))
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = dBGroupSize,
                ModifyDocumentStore = s=>s.Conventions.ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior.RoundRobin
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

                foreach (var node in store.GetRequestExecutor().TopologyNodes.Take(dBGroupSize-1))
                {

                    var i = 0;

                    for (; i<cluster.Nodes.Count; i++)
                    {
                        if (cluster.Nodes[i].ServerStore.NodeTag == node.ClusterTag)
                            break;
                    }

                    await ToggleClusterNodeOnAndOffAndWaitForRehab(databaseName, cluster, i, shouldMaintainElectionTimeout);                    
                }

                Assert.All(cdeArray.GetArray(), cde => Assert.Equal(cde.CurrentCount, SubscriptionsCount));

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

        private async Task ToggleClusterNodeOnAndOffAndWaitForRehab(string databaseName, (List<Raven.Server.RavenServer> Nodes, Raven.Server.RavenServer Leader) cluster, int index, bool shouldMaintainElectionTimeout)
        {
            var node = cluster.Nodes[index];

            node = await ToggleServer(node, shouldMaintainElectionTimeout);
            cluster.Nodes[index] = node;

            await Task.Delay(5000);
            node = await ToggleServer(node, shouldMaintainElectionTimeout);
            cluster.Nodes[index] = node;

            await WaitForRehab(databaseName, cluster);

        }

        private static async Task ContinuouslyGenerateDocs(int DocsBatchSize, DocumentStore store)
        {
            while (false == store.WasDisposed)
            {
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
                catch (Exception ex)
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

            for (int i = 0; i < 10; i++)
            {                
                await DropSubscriptions(databaseName, SubscriptionsCount, cluster);
                
                if (await mainTcs.Task.WaitAsync(1000))
                    break;
            }

            mainTI.Unregister(mainSubscribersCDE.WaitHandle);
        }

        private static async Task WaitForRehab(string dbName, (List<Raven.Server.RavenServer> Nodes, Raven.Server.RavenServer Leader) cluster)
        {
            for (var i = 0; i < cluster.Nodes.Count; i++)
            {
                var curNode = cluster.Nodes[i];
                var rehabCount = 0;
                var attempts = 10;
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
                Assert.True(attempts > 0, "waited for rehab for too long");
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
                catch (Exception)
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

        private async Task<Raven.Server.RavenServer> ToggleServer(Raven.Server.RavenServer node, bool shouldMaintainElectionTimeout)
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

                var dataDir = node.Configuration.Core.DataDirectory.FullPath.Split('/').Last();

                Dictionary<string, string> settings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = node.WebUrl                    
                };
                if (false == shouldMaintainElectionTimeout)
                    settings[RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = node.Configuration.Cluster.ElectionTimeout.AsTimeSpan.TotalMilliseconds.ToString();

                node = base.GetNewServer(new ServerCreationOptions()
                {
                    DeletePrevious = false,
                    RunInMemory = false,
                    CustomSettings = settings,
                    PartialPath = dataDir
                });

            }
            else
            {
                var nodeInfo = await DisposeServerAndWaitForFinishOfDisposalAsync(node);
            }

            return node;
        }
    }
}
