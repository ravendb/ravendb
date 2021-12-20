using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Esprima.Ast;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace StressTests.Rachis
{
    public class SubscriptionsFailoverStress : ClusterTestBase
    {
        public SubscriptionsFailoverStress(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(10);

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

        private static async Task GenerateNewDocuments(IDocumentStore store, int start)
        {
            using (var session = store.OpenAsyncSession())
            {
                for (int i = start; i < start + 10; i++)
                {
                    await session.StoreAsync(new User() {Name = "John" + i, Count = 3}, "Users/" + i);
                }
                await session.SaveChangesAsync().ConfigureAwait(false);
            }
        }

        private static async Task ModifyDocuments(IDocumentStore store, int start)
        {
            using (var session = store.OpenAsyncSession())
            {
                for (int i = start; i < start + 10; i++)
                {
                    var user = await session.LoadAsync<User>("Users/" + i);
                    if (user == null)
                        continue;

                    user.Age++;
                }
                await session.SaveChangesAsync();
            }
        }

        [Fact]
        public async Task SubscriptionFailoverWhileModifying()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            var cts = new CancellationTokenSource();
            try
            {
                var cluster = await CreateRaftCluster(3);
                using (var store = GetDocumentStore(new Options
                {
                    Server = cluster.Leader,
                    ReplicationFactor = 3
                }))
                {
                    var generateTask = Task.Run(async () =>
                    {
                        var newDocs = 0;
                        var modifiedDocs = 0;
                        var killed = false;
                        while (newDocs < 1000)
                        {
                            await GenerateNewDocuments(store, newDocs);
                            await Task.Delay(1000);
                            await ModifyDocuments(store, modifiedDocs);
                            newDocs += 10;
                            modifiedDocs += 3;

                            if (killed == false && newDocs > 600)
                            {
                                await DisposeServerAndWaitForFinishOfDisposalAsync(cluster.Leader);
                                killed = true;
                            }
                        }
                    });

                    var subscriptionName = await store.Subscriptions.CreateAsync<User>(options: new SubscriptionCreationOptions {Query = "from Users where Count > 0"});

                    var strategy = SubscriptionOpeningStrategy.Concurrent;
                    var w1 = CreateWorker(store, subscriptionName, strategy, cts.Token);
                    var w2 = CreateWorker(store, subscriptionName, strategy, cts.Token);
                    var w3 = CreateWorker(store, subscriptionName, strategy, cts.Token);

                    await generateTask;

                    await AssertWaitForTrueAsync(async () =>
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            var result = await session.Query<User>().Where(u => u.Count > 0).ToListAsync(token: cts.Token);
                            return result.Count == 0;
                        }
                    }, timeout: 60_000);
                }
            }
            finally
            {
                cts.Cancel();
            }
        }

        private static long _batchCount;
        private static async Task CreateWorker(DocumentStore store, string subscriptionName, SubscriptionOpeningStrategy strategy = SubscriptionOpeningStrategy.WaitForFree, CancellationToken token = default)
        {
            while (token.IsCancellationRequested == false)
            {
                try
                {
                    using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subscriptionName)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(500),
                        MaxDocsPerBatch = 5,
                        MaxErroneousPeriod = TimeSpan.FromSeconds(5),
                        Strategy = strategy,
                    }))
                    {
                        await subscription.Run((async batch =>
                        {
                            if (Interlocked.Increment(ref _batchCount) % 11 == 0)
                                throw new InvalidOperationException("Random sub failure before");
                          
                            using (var session = batch.OpenAsyncSession())
                            {
                                foreach (var item in batch.Items)
                                {
                                    item.Result.Count--;
                                }

                                await session.SaveChangesAsync(token);
                            }

                            if (Interlocked.Increment(ref _batchCount) % 7 == 0)
                                throw new InvalidOperationException("Random sub failure after");

                        }));
                    }
                }
                catch
                {
                    await Task.Delay(500, token);
                }
            }
        }

        [Fact]
        public async Task SubscriptionShouldFailIfLeaderIsDownAndItIsOnlyOpening()
        {
            const int nodesAmount = 2;
            var (_, leader) = await CreateRaftCluster(nodesAmount);
            var defaultDatabase = "SubscriptionShouldFailIfLeaderIsDown";

            await CreateDatabaseInCluster(defaultDatabase, nodesAmount, leader.WebUrl).ConfigureAwait(false);

            string mentor = Servers.First(x => x.ServerStore.NodeTag != x.ServerStore.LeaderTag).ServerStore.NodeTag;

            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = defaultDatabase
            }.Initialize())
            {
                await GenerateDocuments(store);

                var subscriptionName = await store.Subscriptions.CreateAsync<User>(options: new SubscriptionCreationOptions
                {
                    MentorNode = mentor
                }).ConfigureAwait(false);

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

                await DisposeServerAndWaitForFinishOfDisposalAsync(leader);

                using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subscriptionName)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(500),
                    MaxDocsPerBatch = 20,
                    MaxErroneousPeriod = TimeSpan.FromSeconds(6)
                }))
                {
                    var task = subscription.Run(a => { });
                    Assert.True(await ThrowsAsync<SubscriptionInvalidStateException>(task).WaitWithTimeout(TimeSpan.FromSeconds(60)).ConfigureAwait(false));
                }
            }
        }

        [Fact]
        public async Task SubscriptionShouldFailIfLeaderIsDownBeforeAck()
        {
            const int nodesAmount = 2;
            var (_, leader) = await CreateRaftCluster(nodesAmount);
            var defaultDatabase = "SubscriptionShouldFailIfLeaderIsDownBeforeAck";

            await CreateDatabaseInCluster(defaultDatabase, nodesAmount, leader.WebUrl).ConfigureAwait(false);

            string mentor = Servers.First(x => x.ServerStore.NodeTag != x.ServerStore.LeaderTag).ServerStore.NodeTag;

            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = defaultDatabase
            }.Initialize())
            {
                var subscriptionName = await store.Subscriptions.CreateAsync<User>(options: new SubscriptionCreationOptions
                {
                    MentorNode = mentor
                }).ConfigureAwait(false);

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

                using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subscriptionName)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(500),
                    MaxDocsPerBatch = 20,
                    MaxErroneousPeriod = TimeSpan.FromSeconds(6)
                }))
                {
                    var batchProccessed = new AsyncManualResetEvent();

                    var task = subscription.Run(async a =>
                    {
                        batchProccessed.Set();
                        await DisposeServerAndWaitForFinishOfDisposalAsync(leader);
                    });

                    await GenerateDocuments(store);

                    Assert.True(await batchProccessed.WaitAsync(_reasonableWaitTime));

                    Assert.True(await ThrowsAsync<SubscriptionInvalidStateException>(task).WaitWithTimeout(TimeSpan.FromSeconds(120)).ConfigureAwait(false));
                }
            }
        }

        [Fact]
        public async Task SubscriptionShouldNotFailIfLeaderIsDownButItStillHasEnoughTimeToRetry()
        {
            const int nodesAmount = 2;
            var (_, leader) = await CreateRaftCluster(nodesAmount, shouldRunInMemory: false);
            var indexLeader = Servers.FindIndex(x => x == leader);

            var defaultDatabase = "SubscriptionShouldNotFailIfLeaderIsDownButItStillHasEnoughTimeToRetry";

            await CreateDatabaseInCluster(defaultDatabase, nodesAmount, leader.WebUrl).ConfigureAwait(false);

            string mentor = Servers.First(x => x.ServerStore.NodeTag != x.ServerStore.LeaderTag).ServerStore.NodeTag;

            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = defaultDatabase
            }.Initialize())
            {
                var subscriptionName = await store.Subscriptions.CreateAsync<User>(options: new SubscriptionCreationOptions
                {
                    MentorNode = mentor
                }).ConfigureAwait(false);

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

                using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subscriptionName)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(500),
                    MaxDocsPerBatch = 20,
                    MaxErroneousPeriod = TimeSpan.FromSeconds(120)
                }))
                {
                    var batchProccessed = new AsyncManualResetEvent();
                    var subscriptionRetryBegins = new AsyncManualResetEvent();
                    var batchedAcked = new AsyncManualResetEvent();
                    var disposedOnce = false;

                    subscription.AfterAcknowledgment += x =>
                    {
                        batchedAcked.Set();
                        return Task.CompletedTask;
                    };

                    (string DataDirectory, string Url, string NodeTag) result = default;
                    var task = subscription.Run(batch =>
                    {
                        if (disposedOnce == false)
                        {
                            disposedOnce = true;
                            subscription.OnSubscriptionConnectionRetry += x => subscriptionRetryBegins.SetAndResetAtomically();

                            result = DisposeServerAndWaitForFinishOfDisposal(leader);
                        }
                        batchProccessed.SetAndResetAtomically();
                    });

                    await GenerateDocuments(store);

                    Assert.True(await batchProccessed.WaitAsync(_reasonableWaitTime));
                    Assert.True(await subscriptionRetryBegins.WaitAsync(TimeSpan.FromSeconds(30)));
                    Assert.True(await subscriptionRetryBegins.WaitAsync(TimeSpan.FromSeconds(30)));
                    Assert.True(await subscriptionRetryBegins.WaitAsync(TimeSpan.FromSeconds(30)));

                    leader = Servers[indexLeader] =
                        GetNewServer(new ServerCreationOptions
                        {
                            CustomSettings = new Dictionary<string, string>
                            {
                                {RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), result.Url},
                                {RavenConfiguration.GetKey(x => x.Core.ServerUrls), result.Url}
                            },
                            RunInMemory = false,
                            DeletePrevious = false,
                            DataDirectory = result.DataDirectory
                        });

                    Assert.True(await batchProccessed.WaitAsync(TimeSpan.FromSeconds(60)));
                    Assert.True(await batchedAcked.WaitAsync(TimeSpan.FromSeconds(60)));
                }
            }
        }

        private static async Task ThrowsAsync<T>(Task task) where T : Exception
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
    }
}
