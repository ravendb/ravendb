using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace RachisTests
{
    public class SubscriptionsFailover : ClusterTestBase
    {
        public SubscriptionsFailover(ITestOutputHelper output) : base(output)
        {
        }

        private class SubscriptionProggress
        {
            public int MaxId;
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(60);

        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(10)]
        [InlineData(20)]
        public async Task ContinueFromThePointIStopped(int batchSize)
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            const int nodesAmount = 5;
            var (_, leader) = await CreateRaftCluster(nodesAmount);

            var defaultDatabase = GetDatabaseName();

            await CreateDatabaseInCluster(defaultDatabase, nodesAmount, leader.WebUrl).ConfigureAwait(false);

            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = defaultDatabase
            }.Initialize())
            {
                var reachedMaxDocCountInAckMre = new AsyncManualResetEvent();
                var reachedMaxDocCountInBatchMre = new AsyncManualResetEvent();

                await GenerateDocuments(store);

                (var subscription, var subsTask) = await CreateAndInitiateSubscription(store, defaultDatabase, reachedMaxDocCountInAckMre, reachedMaxDocCountInBatchMre, batchSize);
                await WaitForSubscriptionMreAndAssert(reachedMaxDocCountInBatchMre, subsTask, reachedMaxDocCountInAckMre, iteration: 1);

                await KillServerWhereSubscriptionWorks(defaultDatabase, subscription.SubscriptionName);
                await GenerateDocuments(store);
                await WaitForSubscriptionMreAndAssert(reachedMaxDocCountInBatchMre, subsTask, reachedMaxDocCountInAckMre, iteration: 2);

                await KillServerWhereSubscriptionWorks(defaultDatabase, subscription.SubscriptionName);
                await GenerateDocuments(store);
                await WaitForSubscriptionMreAndAssert(reachedMaxDocCountInBatchMre, subsTask, reachedMaxDocCountInAckMre, iteration: 3);
            }
        }
        [Theory]
        [InlineData(1, 20)]
        [InlineData(5, 10)]
        [InlineData(10, 5)]
        [InlineData(20, 2)]
        public async Task ContinueFromThePointIStoppedConcurrentSubscription(int batchSize, int numberOfConnections)
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            const int nodesAmount = 5;
            var (_, leader) = await CreateRaftCluster(nodesAmount);

            var defaultDatabase = GetDatabaseName();

            await CreateDatabaseInCluster(defaultDatabase, nodesAmount, leader.WebUrl).ConfigureAwait(false);

            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = defaultDatabase
            }.Initialize())
            {
                var reachedMaxDocCountInAckMre = new AsyncManualResetEvent();
                var reachedMaxDocCountInBatchMre = new AsyncManualResetEvent();

                await GenerateDocuments(store);

                (var subscription, var subsTask) = await CreateAndInitiateSubscription(store, defaultDatabase, reachedMaxDocCountInAckMre, reachedMaxDocCountInBatchMre, batchSize, numberOfConnections);

                await WaitForSubscriptionMreAndAssert(reachedMaxDocCountInBatchMre, subsTask, reachedMaxDocCountInAckMre, iteration: 1);

                await KillServerWhereSubscriptionWorks(defaultDatabase, subscription.SubscriptionName);
                await GenerateDocuments(store);
                await WaitForSubscriptionMreAndAssert(reachedMaxDocCountInBatchMre, subsTask, reachedMaxDocCountInAckMre, iteration: 2);

                await KillServerWhereSubscriptionWorks(defaultDatabase, subscription.SubscriptionName);
                await GenerateDocuments(store);
                await WaitForSubscriptionMreAndAssert(reachedMaxDocCountInBatchMre, subsTask, reachedMaxDocCountInAckMre, iteration: 3);
            }
        }

        [Fact]
        public async Task SubscripitonDeletionFromCluster()
        {
            const int nodesAmount = 5;
            var (_, leader) = await CreateRaftCluster(nodesAmount);

            var defaultDatabase = GetDatabaseName();

            await CreateDatabaseInCluster(defaultDatabase, nodesAmount, leader.WebUrl).ConfigureAwait(false);

            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = defaultDatabase
            }.Initialize())
            {
                var usersCount = new List<User>();
                var reachedMaxDocCountMre = new AsyncManualResetEvent();

                var subscriptionId = await store.Subscriptions.CreateAsync<User>();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Peter"
                    });
                    await session.SaveChangesAsync();
                }

                var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subscriptionId)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });

                subscription.AfterAcknowledgment += b => { reachedMaxDocCountMre.Set(); return Task.CompletedTask; };

                GC.KeepAlive(subscription.Run(x => { }));

                Assert.True(await reachedMaxDocCountMre.WaitAsync(TimeSpan.FromSeconds(60)));

                foreach (var ravenServer in Servers)
                {
                    using (ravenServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        Assert.NotNull(ravenServer.ServerStore.Cluster.Read(context, SubscriptionState.GenerateSubscriptionItemKeyName(defaultDatabase, subscriptionId.ToString())));
                    }
                }

                await subscription.DisposeAsync();

                var deleteResult = store.Maintenance.Server.Send(new DeleteDatabasesOperation(defaultDatabase, hardDelete: true));

                foreach (var ravenServer in Servers)
                {
                    await ravenServer.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, deleteResult.RaftCommandIndex).WaitWithTimeout(TimeSpan.FromSeconds(60));
                }

                await Task.Delay(2000);

                foreach (var ravenServer in Servers)
                {
                    using (ravenServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        Assert.Null(ravenServer.ServerStore.Cluster.Read(context, SubscriptionState.GenerateSubscriptionItemKeyName(defaultDatabase, subscriptionId.ToString())));
                    }
                }
            }
        }

        [Fact]
        public async Task SetMentorToSubscriptionWithFailover()
        {
            const int nodesAmount = 5;
            var (_, leader) = await CreateRaftCluster(nodesAmount);

            var defaultDatabase = GetDatabaseName();

            await CreateDatabaseInCluster(defaultDatabase, nodesAmount, leader.WebUrl).ConfigureAwait(false);

            string mentor = "C";
            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = defaultDatabase
            }.Initialize())
            {
                var reachedMaxDocCountInAckMre = new AsyncManualResetEvent();
                var reachedMaxDocCountInBatchMre = new AsyncManualResetEvent();

                await GenerateDocuments(store);

                (var subscription, var subsTask) = await CreateAndInitiateSubscription(store, defaultDatabase, reachedMaxDocCountInAckMre, reachedMaxDocCountInBatchMre, 20, mentor: mentor);

                await WaitForSubscriptionMreAndAssert(reachedMaxDocCountInBatchMre, subsTask, reachedMaxDocCountInAckMre, iteration: 1);

                await KillServerWhereSubscriptionWorks(defaultDatabase, subscription.SubscriptionName);
                await GenerateDocuments(store);
                await WaitForSubscriptionMreAndAssert(reachedMaxDocCountInBatchMre, subsTask, reachedMaxDocCountInAckMre, iteration: 2);

                await KillServerWhereSubscriptionWorks(defaultDatabase, subscription.SubscriptionName);
                await GenerateDocuments(store);
                await WaitForSubscriptionMreAndAssert(reachedMaxDocCountInBatchMre, subsTask, reachedMaxDocCountInAckMre, iteration: 3);
            }
        }

        private async Task WaitForSubscriptionMreAndAssert(AsyncManualResetEvent reachedMaxDocCountInBatchMre, Task subsTask, AsyncManualResetEvent reachedMaxDocCountInAckMre, int iteration)
        {
            if (await reachedMaxDocCountInBatchMre.WaitAsync(_reasonableWaitTime) == false)
            {
                Assert.False(subsTask.IsFaulted, $"{iteration}. Reached in batch {BatchCounter}/10 & Subscription failed: {subsTask?.Exception?.ToString()}");
                Assert.True(false, $"{iteration}. Reached in batch {BatchCounter}/10");
            }

            if (await reachedMaxDocCountInAckMre.WaitAsync(_reasonableWaitTime) == false)
            {
                Assert.False(subsTask.IsFaulted, $"{iteration}. Reached in ack {AckCounter}/10 & Subscription failed: {subsTask?.Exception?.ToString()}");
                Assert.True(false, $"{iteration}. Reached in ack {AckCounter}/10");
            }

            Assert.False(subsTask.IsFaulted, $"{iteration}. Reached in batch {BatchCounter}/10, Reached in ack {AckCounter}/10 & Subscription failed: {subsTask?.Exception?.ToString()}");
            Assert.False(GotUnexpectedId, "GotUnexpectedId on Client ACK");

            reachedMaxDocCountInAckMre.Reset();
            reachedMaxDocCountInBatchMre.Reset();
            Interlocked.Exchange(ref BatchCounter, 0);
            Interlocked.Exchange(ref AckCounter, 0);
        }

        [MultiplatformTheory(RavenArchitecture.AllX64)]
        [InlineData(3)]
        [InlineData(5)]
        public async Task DistributedRevisionsSubscription(int nodesAmount)
        {
            var uniqueRevisions = new HashSet<string>();
            var uniqueDocs = new HashSet<string>();

            var (_, leader) = await CreateRaftCluster(nodesAmount).ConfigureAwait(false);

            var defaultDatabase = GetDatabaseName();

            await CreateDatabaseInCluster(defaultDatabase, nodesAmount, leader.WebUrl).ConfigureAwait(false);

            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = defaultDatabase
            }.Initialize())
            {
                await SetupRevisions(leader, defaultDatabase).ConfigureAwait(false);

                var reachedMaxDocCountMre = new AsyncManualResetEvent();
                var ackSent = new AsyncManualResetEvent();

                var continueMre = new AsyncManualResetEvent();

                await GenerateDistributedRevisionsDataAsync(defaultDatabase);

                var subscriptionId = await store.Subscriptions.CreateAsync<Revision<User>>().ConfigureAwait(false);

                var docsCount = 0;
                var revisionsCount = 0;
                var expectedRevisionsCount = 0;
                SubscriptionWorker<Revision<User>> subscription = null;
                int i;
                for (i = 0; i < 10; i++)
                {
                    subscription = store.Subscriptions.GetSubscriptionWorker<Revision<User>>(new SubscriptionWorkerOptions(subscriptionId)
                    {
                        MaxErroneousPeriod = nodesAmount == 5 ? TimeSpan.FromSeconds(15) : TimeSpan.FromSeconds(5),
                        MaxDocsPerBatch = 1,
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(100)
                    });

                    subscription.AfterAcknowledgment += async b =>
                    {
                        Assert.True(await continueMre.WaitAsync(TimeSpan.FromSeconds(60)));

                        try
                        {
                            if (revisionsCount == expectedRevisionsCount)
                            {
                                continueMre.Reset();
                                ackSent.Set();
                            }

                            Assert.True(await continueMre.WaitAsync(TimeSpan.FromSeconds(60)));
                        }
                        catch (Exception)
                        {
                        }
                    };
                    var started = new AsyncManualResetEvent();

                    var task = subscription.Run(b =>
                    {
                        started.Set();
                        HandleSubscriptionBatch(nodesAmount, b, uniqueDocs, ref docsCount, uniqueRevisions, reachedMaxDocCountMre, ref revisionsCount);
                    });
                    var cont = task.ContinueWith(t =>
                    {
                        reachedMaxDocCountMre.SetException(t.Exception);
                        ackSent.SetException(t.Exception);
                    }, TaskContinuationOptions.OnlyOnFaulted);

                    await Task.WhenAny(task, started.WaitAsync(TimeSpan.FromSeconds(60)));

                    if (started.IsSet)
                        break;

                    Assert.IsType<SubscriptionDoesNotExistException>(task.Exception.InnerException);

                    subscription.Dispose();
                }

                Assert.NotEqual(i, 10);

                expectedRevisionsCount = nodesAmount + 2;
                continueMre.Set();

                Assert.True(await ackSent.WaitAsync(_reasonableWaitTime).ConfigureAwait(false), $"Doc count is {docsCount} with revisions {revisionsCount}/{expectedRevisionsCount} (1st assert)");
                ackSent.Reset(true);

                var disposedTag = await KillServerWhereSubscriptionWorks(defaultDatabase, subscription.SubscriptionName).ConfigureAwait(false);
                await WaitForResponsibleNodeToChange(defaultDatabase, subscription.SubscriptionName, disposedTag);

                continueMre.Set();
                expectedRevisionsCount += 2;

                Assert.True(await ackSent.WaitAsync(_reasonableWaitTime).ConfigureAwait(false), $"Doc count is {docsCount} with revisions {revisionsCount}/{expectedRevisionsCount} (2nd assert)");
                ackSent.Reset(true);
                continueMre.Set();

                expectedRevisionsCount = (int)Math.Pow(nodesAmount, 2);
                if (nodesAmount == 5)
                {
                    var secondDisposedTag = await KillServerWhereSubscriptionWorks(defaultDatabase, subscription.SubscriptionName).ConfigureAwait(false);
                    await WaitForResponsibleNodeToChange(defaultDatabase, subscription.SubscriptionName, secondDisposedTag);
                }

                Assert.True(await reachedMaxDocCountMre.WaitAsync(_reasonableWaitTime).ConfigureAwait(false), $"Doc count is {docsCount} with revisions {revisionsCount}/{expectedRevisionsCount} (3rd assert)");
            }
        }

        [MultiplatformFact(RavenArchitecture.AllX86)]
        public async Task DistributedRevisionsSubscription32Bit()
        {
            await DistributedRevisionsSubscription(3);
        }

        private static void HandleSubscriptionBatch(int nodesAmount, SubscriptionBatch<Revision<User>> b, HashSet<string> uniqueDocs, ref int docsCount, HashSet<string> uniqueRevisions,
            AsyncManualResetEvent reachedMaxDocCountMre, ref int revisionsCount)
        {
            foreach (var item in b.Items)
            {
                var x = item.Result;
                try
                {
                    if (x == null)
                    {
                    }
                    else if (x.Previous == null)
                    {
                        if (uniqueDocs.Add(x.Current.Id))
                            docsCount++;
                        if (uniqueRevisions.Add(x.Current.Name))
                            revisionsCount++;
                    }
                    else if (x.Current == null)
                    {
                    }
                    else
                    {
                        if (x.Current.Age > x.Previous.Age)
                        {
                            if (uniqueRevisions.Add(x.Current.Name))
                                revisionsCount++;
                        }
                    }

                    if (docsCount == nodesAmount && revisionsCount == Math.Pow(nodesAmount, 2))
                        reachedMaxDocCountMre.Set();
                }
                catch (Exception)
                {
                }
            }
        }

        private async Task SetupRevisions(RavenServer server, string defaultDatabase)
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 10,
                    },
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        ["Users"] = new RevisionsCollectionConfiguration
                        {
                            Disabled = false,
                            MinimumRevisionsToKeep = 10
                        }
                    }
                };

                var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(defaultDatabase);
                var res = await documentDatabase.ServerStore.ModifyDatabaseRevisions(context,
                    defaultDatabase,
                    DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(configuration, context), Guid.NewGuid().ToString());

                foreach (var s in Servers)// need to wait for it on all servers
                {
                    documentDatabase = await s.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(defaultDatabase);
                    await documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(res.Item1, s.ServerStore.Engine.OperationTimeout);
                }
            }
        }

        private async Task GenerateDistributedRevisionsDataAsync(string defaultDatabase)
        {
            IReadOnlyList<ServerNode> nodes;
            using (var store = new DocumentStore
            {
                Urls = new[] { Servers[0].WebUrl },
                Database = defaultDatabase
            }.Initialize())
            {
                await store.GetRequestExecutor().UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(new ServerNode
                {
                    Url = store.Urls[0],
                    Database = defaultDatabase,
                })
                {
                    TimeoutInMs = Timeout.Infinite
                });

                nodes = store.GetRequestExecutor().TopologyNodes;
            }
            var rnd = new Random(1);
            for (var index = 0; index < Servers.Count; index++)
            {
                var curVer = 0;
                foreach (var server in Servers.OrderBy(x => rnd.Next()))
                {
                    using (var curStore = new DocumentStore
                    {
                        Urls = new[] { server.WebUrl },
                        Database = defaultDatabase,
                        Conventions = new DocumentConventions
                        {
                            DisableTopologyUpdates = true
                        }
                    }.Initialize())
                    {
                        var curDocName = $"user {index} revision {curVer}";
                        using (var session = (DocumentSession)curStore.OpenSession())
                        {
                            if (curVer == 0)
                            {
                                session.Store(new User
                                {
                                    Name = curDocName,
                                    Age = curVer,
                                    Id = $"users/{index}"
                                }, $"users/{index}");
                            }
                            else
                            {
                                var user = session.Load<User>($"users/{index}");
                                user.Age = curVer;
                                user.Name = curDocName;
                                session.Store(user, $"users/{index}");
                            }

                            session.SaveChanges();

                            Assert.True(
                                AsyncHelpers.RunSync(() => WaitForDocumentInClusterAsync<User>(nodes, "users/" + index, x => x.Name == curDocName, _reasonableWaitTime))
                                );
                        }
                    }
                    curVer++;
                }
            }
        }

        private int AckCounter = 0;
        private int BatchCounter = 0;
        private bool GotUnexpectedId = false;
        private async Task<(SubscriptionWorker<User> worker, Task subsTask)> CreateAndInitiateSubscription(IDocumentStore store, string defaultDatabase,
            AsyncManualResetEvent reachedMaxDocCountInAckMre, AsyncManualResetEvent reachedMaxDocCountInBatchMre, int batchSize, int numberOfConnections = 1, string mentor = null)
        {
            var proggress = new SubscriptionProggress()
            {
                MaxId = 0
            };
            var subscriptionName = await store.Subscriptions.CreateAsync<User>(options: new SubscriptionCreationOptions
            {
                MentorNode = mentor
            }).ConfigureAwait(false);

            SubscriptionWorker<User> subscription;
            List<Task> subTasks = new();
            int connections = numberOfConnections;
            var strategy = numberOfConnections > 1 ? SubscriptionOpeningStrategy.Concurrent : SubscriptionOpeningStrategy.OpenIfFree;
            do
            {
                subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subscriptionName)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(500), 
                    MaxDocsPerBatch = batchSize,
                    Strategy = strategy
                });
                subscription.AfterAcknowledgment += b =>
                {
                    foreach (var item in b.Items)
                    {
                        if (strategy != SubscriptionOpeningStrategy.Concurrent)
                        {
                            var x = item.Result;
                            var afterSlash = x.Id.Substring(x.Id.LastIndexOf("/", StringComparison.OrdinalIgnoreCase) + 1);
                            int curId = int.Parse(afterSlash.Substring(0, afterSlash.Length - 2));
                            if (curId < proggress.MaxId)
                                GotUnexpectedId = true;
                            else
                            {
                                proggress.MaxId = curId;
                            }
                        }

                        if (Interlocked.Increment(ref AckCounter) == 10)
                        {
                            reachedMaxDocCountInAckMre.Set();
                        }
                    }

                    return Task.CompletedTask;
                };
                
                subTasks.Add(subscription.Run(batch =>
                {
                    foreach (var item in batch.Items)
                    {
                        if (Interlocked.Increment(ref BatchCounter) == 10)
                        {
                            reachedMaxDocCountInBatchMre.Set();
                        }
                    }

                }));
                connections--;
            } while (connections > 0);

            var subscripitonState = await store.Subscriptions.GetSubscriptionStateAsync(subscriptionName, store.Database);
            var getDatabaseTopologyCommand = new GetDatabaseRecordOperation(defaultDatabase);
            var record = await store.Maintenance.Server.SendAsync(getDatabaseTopologyCommand).ConfigureAwait(false);

            foreach (var server in Servers.Where(s => record.Topology.RelevantFor(s.ServerStore.NodeTag)))
            {
                await server.ServerStore.Cluster.WaitForIndexNotification(subscripitonState.SubscriptionId).ConfigureAwait(false);
            }

            if (mentor != null)
            {
                Assert.Equal(mentor, record.Topology.WhoseTaskIsIt(RachisState.Follower, subscripitonState, null));
            }
            
            //await Task.WhenAny(task, Task.Delay(_reasonableWaitTime)).ConfigureAwait(false);

            return (subscription, Task.WhenAll(subTasks));
        }

        private async Task<string> KillServerWhereSubscriptionWorks(string defaultDatabase, string subscriptionName)
        {
            var sp = Stopwatch.StartNew();
            try
            {
                while (sp.ElapsedMilliseconds < _reasonableWaitTime.TotalMilliseconds)
                {
                    string tag = null;
                    var someServer = Servers.First(x => x.Disposed == false);
                    using (someServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var databaseRecord = someServer.ServerStore.Cluster.ReadDatabase(context, defaultDatabase);
                        var db = await someServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(defaultDatabase).ConfigureAwait(false);
                        var subscriptionState = db.SubscriptionStorage.GetSubscriptionFromServerStore(subscriptionName);
                        tag = databaseRecord.Topology.WhoseTaskIsIt(someServer.ServerStore.Engine.CurrentState, subscriptionState, null);
                    }
                    if (tag == null)
                    {
                        await Task.Delay(100);
                        continue;
                    }

                    var server = Servers.First(x => x.ServerStore.NodeTag == tag);

                    if (server.Disposed)
                    {
                        await Task.Delay(100);
                        continue;
                    }
                    await DisposeServerAndWaitForFinishOfDisposalAsync(server);
                    return tag;
                }

                return null;
            }
            finally
            {
                Assert.True(sp.ElapsedMilliseconds < _reasonableWaitTime.TotalMilliseconds);
            }
        }

        private async Task WaitForResponsibleNodeToChange(string database, string subscriptionName, string responsibleNode)
        {
            var sp = Stopwatch.StartNew();
            try
            {
                while (sp.ElapsedMilliseconds < _reasonableWaitTime.TotalMilliseconds)
                {
                    string tag;
                    var someServer = Servers.First(x => x.Disposed == false);
                    using (someServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var databaseRecord = someServer.ServerStore.Cluster.ReadDatabase(context, database);
                        var db = await someServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database).ConfigureAwait(false);
                        var subscriptionState = db.SubscriptionStorage.GetSubscriptionFromServerStore(subscriptionName);
                        tag = databaseRecord.Topology.WhoseTaskIsIt(someServer.ServerStore.Engine.CurrentState, subscriptionState, null);
                    }

                    if (tag == null || tag == responsibleNode)
                    {
                        await Task.Delay(333);
                        continue;
                    }

                    return;
                }
            }
            finally
            {
                Assert.True(sp.ElapsedMilliseconds < _reasonableWaitTime.TotalMilliseconds);
            }
        }

        private static async Task GenerateDocuments(IDocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < 10; i++)
                {
                    await session.StoreAsync(new User()
                    {
                        Name = "John" + i
                    })
                        .ConfigureAwait(false);
                }
                await session.SaveChangesAsync().ConfigureAwait(false);
            }
        }

        [Fact]
        public async Task SubscriptionWorkerShouldNotFailoverToErroredNodes()
        {
            var cluster = await CreateRaftCluster(numberOfNodes: 3);

            using (var store = GetDocumentStore(new Options
            {
                ReplicationFactor = 3,
                Server = cluster.Leader,
                DeleteDatabaseOnDispose = false
            }))
            {
                Servers.ForEach(x => x.ForTestingPurposesOnly().GatherVerboseDatabaseDisposeInformation = true);

                var mre = new AsyncManualResetEvent();
                using (var subscriptionManager = new DocumentSubscriptions(store))
                {
                    var reqEx = store.GetRequestExecutor();
                    var name = subscriptionManager.Create(new SubscriptionCreationOptions<User>());

                    var subs = await SubscriptionFailoverWithWaitingChains.GetSubscription(name, store.Database, cluster.Nodes);
                    Assert.NotNull(subs);
                    await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(subs.SubscriptionId, cluster.Nodes);

                    await ActionWithLeader(async l => await WaitForResponsibleNode(l.ServerStore, store.Database, name, toBecomeNull: false));

                    Assert.True(WaitForValue(() => reqEx.Topology != null, true));
                    var topology = reqEx.Topology;
                    var serverNode1 = topology.Nodes[0];
                    await reqEx.UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(serverNode1) { TimeoutInMs = 10_000 });
                    var node1 = Servers.First(x => x.WebUrl.Equals(serverNode1.Url, StringComparison.InvariantCultureIgnoreCase));
                    var (_, __, disposedTag) = await DisposeServerAndWaitForFinishOfDisposalAsync(node1);
                    var serverNode2 = topology.Nodes[1];
                    var node2 = Servers.First(x => x.WebUrl.Equals(serverNode2.Url, StringComparison.InvariantCultureIgnoreCase));
                    var (_, ___, disposedTag2) = await DisposeServerAndWaitForFinishOfDisposalAsync(node2);
                    var onlineServer = cluster.Nodes.Single(x => x.ServerStore.NodeTag != disposedTag && x.ServerStore.NodeTag != disposedTag2).ServerStore;
                    await WaitForResponsibleNode(onlineServer, store.Database, name, toBecomeNull: true);

                    using (reqEx.ContextPool.AllocateOperationContext(out var context))
                    {
                        var command = new KillOperationCommand(-int.MaxValue);
                        await Assert.ThrowsAsync<RavenException>(async () => await reqEx.ExecuteAsync(command, context));
                    }
                    var redirects = new Dictionary<string, string>();
                    var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(name)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(16),
                        MaxErroneousPeriod = TimeSpan.FromMinutes(5)
                    });
                    subscription.OnSubscriptionConnectionRetry += ex =>
                    {
                        if (string.IsNullOrEmpty(subscription.CurrentNodeTag))
                            return;

                        redirects[subscription.CurrentNodeTag] = ex.ToString();
                        mre.Set();
                    };
                    var expectedTag = onlineServer.NodeTag;
                    _ = subscription.Run(x => { });
                    Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(60)), $"Could not redirect to alive node in time{Environment.NewLine}Redirects:{Environment.NewLine}{string.Join(Environment.NewLine, redirects.Select(x => $"Tag: {x.Key}, Exception: {x.Value}").ToList())}");

                    Assert.True(redirects.Keys.Contains(expectedTag), $"Could not find '{expectedTag}' in Redirects:{Environment.NewLine}{string.Join(Environment.NewLine, redirects.Select(x => $"Tag: {x.Key}, Exception: {x.Value}").ToList())}");
                    Assert.False(redirects.Keys.Contains(disposedTag), $"Found disposed '{disposedTag}' in Redirects:{Environment.NewLine}{string.Join(Environment.NewLine, redirects.Select(x => $"Tag: {x.Key}, Exception: {x.Value}").ToList())}");
                    Assert.False(redirects.Keys.Contains(disposedTag2), $"Found disposed '{disposedTag2}' in Redirects:{Environment.NewLine}{string.Join(Environment.NewLine, redirects.Select(x => $"Tag: {x.Key}, Exception: {x.Value}").ToList())}");
                    Assert.Equal(1, redirects.Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task SubscriptionWorkerShouldStayOnCandidateNodes()
        {
            var cluster = await CreateRaftCluster(numberOfNodes: 2, watcherCluster: false, shouldRunInMemory: false);

            using (var store = GetDocumentStore(new Options
            {
                ReplicationFactor = 2,
                Server = cluster.Leader,
                DeleteDatabaseOnDispose = false,
                RunInMemory = false
            }))
            {
                Servers.ForEach(x => x.ForTestingPurposesOnly().GatherVerboseDatabaseDisposeInformation = true);

                var mre = new AsyncManualResetEvent();
                using (var subscriptionManager = new DocumentSubscriptions(store))
                {
                    var name = await subscriptionManager.CreateAsync(new SubscriptionCreationOptions<User>());

                    var subs = await SubscriptionFailoverWithWaitingChains.GetSubscription(name, store.Database, cluster.Nodes);
                    Assert.NotNull(subs);
                    await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(subs.SubscriptionId, cluster.Nodes);
                    string tag = null;
                    string leadertag = null;
                    await ActionWithLeader(async l =>
                    {
                        leadertag = l.ServerStore.NodeTag;
                        tag = await WaitForResponsibleNode(l.ServerStore, store.Database, name, toBecomeNull: false);
                    });

                    Assert.NotNull(tag);

                    var redirects = new Dictionary<string, string>();
                    var processedIds = new HashSet<string>();
                    var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(name)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(16),
                        MaxErroneousPeriod = TimeSpan.FromMinutes(5)
                    });

                    subscription.OnEstablishedSubscriptionConnection += () =>
                    {
                        mre.Set();
                    };
                    subscription.AfterAcknowledgment += batch =>
                    {
                        foreach (var item in batch.Items.Select(x => x.Id))
                        {
                            processedIds.Add(item);
                        }
                        return Task.CompletedTask;
                    };
                    _ = subscription.Run(x => { });

                    Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(60)), $"Could not connect subscription in time{Environment.NewLine}Redirects:{Environment.NewLine}{string.Join(Environment.NewLine, redirects.Select(x => $"Tag: {x.Key}, Exception: {x.Value}").ToList())}");

                    subscription.OnSubscriptionConnectionRetry += ex =>
                    {
                        if (string.IsNullOrEmpty(subscription.CurrentNodeTag))
                            return;

                        redirects[subscription.CurrentNodeTag] = ex.ToString();
                    };

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                            Name = "EGOR"
                        }, "users/322");

                        session.SaveChanges();
                    }

                    await WaitAndAssertForValueAsync(() => processedIds.Contains("users/322"), true);

                    var sp = Stopwatch.StartNew();
                    var toDispose = Servers.FirstOrDefault(x => x.ServerStore.NodeTag != tag);
                    Assert.NotNull(toDispose);

                    var (_, __, disposedTag) = await DisposeServerAndWaitForFinishOfDisposalAsync(toDispose);
                    Assert.Equal(toDispose.ServerStore.NodeTag, disposedTag);
                    var processingNode = Servers.FirstOrDefault(x => x.ServerStore.NodeTag == tag);
                    Assert.NotNull(processingNode);

                    // wait for the node to become candidate
                    await WaitAndAssertForValueAsync(() => processingNode.ServerStore.Engine.CurrentState, RachisState.Candidate);

                    var db = await Databases.GetDocumentDatabaseInstanceFor(processingNode.ServerStore.NodeTag, store.Database);
                    var testingStuff = db.ForTestingPurposesOnly();
                    bool shouldWaitForClusterStabilization = false;
                    var subscriptionInterrupt = new AsyncManualResetEvent();
                    using (testingStuff.CallDuringWaitForChangedDocuments(() =>
                           {
                               shouldWaitForClusterStabilization = db.SubscriptionStorage.ShouldWaitForClusterStabilization();
                               subscriptionInterrupt.Set();
                           }))
                    {
                        await subscriptionInterrupt.WaitAsync(_reasonableWaitTime);
                        // wait for the wait for cluster stabilization in subscription connection to happen
                        Assert.True(shouldWaitForClusterStabilization, "shouldWaitForClusterStabilization");
                    }

                    var resurrectedNode = await ToggleServer(toDispose, false, base.GetNewServer);

                    // wait for nodes to stabilize
                    await WaitAndAssertForValueAsync(() =>
                    {
                        if (processingNode.ServerStore.Engine.CurrentState == RachisState.Candidate || resurrectedNode.ServerStore.Engine.CurrentState == RachisState.Candidate)
                            return false;

                        if (processingNode.ServerStore.Engine.CurrentState == RachisState.Passive || resurrectedNode.ServerStore.Engine.CurrentState == RachisState.Passive)
                            return false;

                        return true;
                    }, true, timeout: 60_000, interval: 333);
                    string responsibleNodeAfterClusterStabilization = null;
                    responsibleNodeAfterClusterStabilization = await WaitForResponsibleNode(processingNode.ServerStore, store.Database, name, toBecomeNull: false);
                    Assert.Equal(tag, responsibleNodeAfterClusterStabilization);

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                            Name = "EGR"
                        }, "users/228");

                        session.SaveChanges();
                    }

                    await WaitAndAssertForValueAsync(() => processedIds.Contains("users/228"), true);
                    Assert.Empty(redirects);
                }
            }
        }

        private async Task<string> WaitForResponsibleNode(ServerStore online, string dbName, string subscriptionName, bool toBecomeNull = false)
        {
            var sp = Stopwatch.StartNew();
            try
            {
                while (sp.ElapsedMilliseconds < 60_000)
                {
                    string tag;
                    using (online.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var databaseRecord = online.Cluster.ReadDatabase(context, dbName);
                        var db = await online.DatabasesLandlord.TryGetOrCreateResourceStore(dbName).ConfigureAwait(false);
                        var subscriptionState = db.SubscriptionStorage.GetSubscriptionFromServerStore(subscriptionName);
                        tag = databaseRecord.Topology.WhoseTaskIsIt(online.Engine.CurrentState, subscriptionState, null);
                    }

                    if (toBecomeNull)
                    {
                        if (tag != null)
                        {
                            await Task.Delay(333);
                            continue;
                        }
                    }
                    else
                    {
                        if (tag == null)
                        {
                            await Task.Delay(333);
                            continue;
                        }
                    }

                    return tag;
                }
            }
            finally
            {
                sp.Stop();
                Assert.True(sp.ElapsedMilliseconds < _reasonableWaitTime.TotalMilliseconds);
            }

            return null;
        }

        protected static async Task ThrowsAsync<T>(Task task) where T : Exception
        {
            var threw = false;
            try
            {
                await task.ConfigureAwait(false);
            }
            catch (T)
            {
                threw = true;
            }
            catch (Exception ex)
            {
                threw = true;
                throw new ThrowsException(typeof(T), ex);
            }
            finally
            {
                if (threw == false)
                    throw new ThrowsException(typeof(T));
            }
        }

        internal static async Task<RavenServer> ToggleServer(Raven.Server.RavenServer node, bool shouldTrapRevivedNodesIntoCandidate, Func<ServerCreationOptions, string, RavenServer> getNewServerFunc)
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

                node = getNewServerFunc(new ServerCreationOptions()
                    {
                        DeletePrevious = false,
                        RunInMemory = false,
                        CustomSettings = settings,
                        DataDirectory = dataDirectory
                    }, $"{node.DebugTag}-{nameof(ToggleServer)}");

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
