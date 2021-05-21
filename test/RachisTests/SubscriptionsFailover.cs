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
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
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

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(10);

        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(10)]
        [InlineData(20)]
        public async Task ContinueFromThePointIStopped(int batchSize)
        {
            const int nodesAmount = 5;
            var leader = await this.CreateRaftClusterAndGetLeader(nodesAmount);

            var defaultDatabase = "ContinueFromThePointIStopped";

            await CreateDatabaseInCluster(defaultDatabase, nodesAmount, leader.WebUrl).ConfigureAwait(false);

            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = defaultDatabase
            }.Initialize())
            {
                var usersCount = new List<User>();
                var reachedMaxDocCountMre = new AsyncManualResetEvent();

                await GenerateDocuments(store);

                (var subscription, var subsTask) = await CreateAndInitiateSubscription(store, defaultDatabase, usersCount, reachedMaxDocCountMre, batchSize);

                Assert.True(await Task.WhenAny(subsTask, reachedMaxDocCountMre.WaitAsync(TimeSpan.FromSeconds(60))).WaitAsync(_reasonableWaitTime), $"Reached {usersCount.Count}/10");
                Assert.False(subsTask.IsFaulted, subsTask?.Exception?.ToString());

                usersCount.Clear();
                reachedMaxDocCountMre.Reset();
                var sp = Stopwatch.StartNew();
                await KillServerWhereSubscriptionWorks(defaultDatabase, subscription.SubscriptionName);

                await GenerateDocuments(store);

                Assert.True(await Task.WhenAny(subsTask, reachedMaxDocCountMre.WaitAsync(TimeSpan.FromSeconds(60))).WaitAsync(_reasonableWaitTime), $"Reached {usersCount.Count}/10");
                Assert.False(subsTask.IsFaulted, subsTask?.Exception?.ToString());

                usersCount.Clear();
                reachedMaxDocCountMre.Reset();

                await KillServerWhereSubscriptionWorks(defaultDatabase, subscription.SubscriptionName);

                await GenerateDocuments(store);

                Assert.True(await Task.WhenAny(subsTask, reachedMaxDocCountMre.WaitAsync(TimeSpan.FromSeconds(60))).WaitAsync(_reasonableWaitTime), $"Reached {usersCount.Count}/10");
                Assert.False(subsTask.IsFaulted, subsTask?.Exception?.ToString());
            }
        }

        [Fact]
        public async Task SubscripitonDeletionFromCluster()
        {
            const int nodesAmount = 5;
            var leader = await this.CreateRaftClusterAndGetLeader(nodesAmount);

            var defaultDatabase = "ContinueFromThePointIStopped";

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
                    await ravenServer.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, deleteResult.RaftCommandIndex + nodesAmount).WaitWithTimeout(TimeSpan.FromSeconds(60));
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

        public async Task CreateDistributedRevisions()
        {
            const int nodesAmount = 5;
            var leader = await this.CreateRaftClusterAndGetLeader(nodesAmount).ConfigureAwait(false);

            var defaultDatabase = "DistributedRevisionsSubscription";

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

                for (int i = 0; i < 17; i++)
                {
                    await GenerateDistributedRevisionsDataAsync(defaultDatabase);
                }
            }
        }

        [Fact]
        public async Task SetMentorToSubscriptionWithFailover()
        {
            const int nodesAmount = 5;
            var leader = await CreateRaftClusterAndGetLeader(nodesAmount);

            var defaultDatabase = "SetMentorToSubscription";

            await CreateDatabaseInCluster(defaultDatabase, nodesAmount, leader.WebUrl).ConfigureAwait(false);

            string mentor = "C";
            string tag2, tag3;
            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = defaultDatabase
            }.Initialize())
            {
                var usersCount = new List<User>();
                var reachedMaxDocCountMre = new AsyncManualResetEvent();

                await GenerateDocuments(store);

                (var subscription, var subsTask) = await CreateAndInitiateSubscription(store, defaultDatabase, usersCount, reachedMaxDocCountMre, 20, mentor: mentor);

                Assert.True(await reachedMaxDocCountMre.WaitAsync(_reasonableWaitTime), $"Reached {usersCount.Count}/10");

                usersCount.Clear();
                reachedMaxDocCountMre.Reset();

                await KillServerWhereSubscriptionWorks(defaultDatabase, subscription.SubscriptionName);

                await GenerateDocuments(store);

                Assert.True(await reachedMaxDocCountMre.WaitAsync(_reasonableWaitTime), $"Reached {usersCount.Count}/10");

                tag2 = subscription.CurrentNodeTag;

                //     Assert.NotEqual(tag1,tag2);
                usersCount.Clear();
                reachedMaxDocCountMre.Reset();

                await KillServerWhereSubscriptionWorks(defaultDatabase, subscription.SubscriptionName);

                await GenerateDocuments(store);

                Assert.True(await reachedMaxDocCountMre.WaitAsync(_reasonableWaitTime), $"Reached {usersCount.Count}/10");

                tag3 = subscription.CurrentNodeTag;
                // Assert.NotEqual(tag1, tag3);
                //    Assert.NotEqual(tag2, tag3);
            }
        }

        [Theory64Bit]
        [InlineData(3)]
        [InlineData(5)]
        public async Task DistributedRevisionsSubscription(int nodesAmount)
        {
            var uniqueRevisions = new HashSet<string>();
            var uniqueDocs = new HashSet<string>();

            var leader = await CreateRaftClusterAndGetLeader(nodesAmount).ConfigureAwait(false);

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

        [Fact32Bit]
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

        private async Task<(SubscriptionWorker<User> worker, Task subsTask)> CreateAndInitiateSubscription(IDocumentStore store, string defaultDatabase, List<User> usersCount, AsyncManualResetEvent reachedMaxDocCountMre, int batchSize, string mentor = null)
        {
            var proggress = new SubscriptionProggress()
            {
                MaxId = 0
            };
            var subscriptionName = await store.Subscriptions.CreateAsync<User>(options: new SubscriptionCreationOptions
            {
                MentorNode = mentor
            }).ConfigureAwait(false);

            var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subscriptionName)
            {
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(500),
                MaxDocsPerBatch = batchSize
            });
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

            subscription.AfterAcknowledgment += b =>
            {
                try
                {
                    foreach (var item in b.Items)
                    {
                        var x = item.Result;
                        int curId = 0;
                        var afterSlash = x.Id.Substring(x.Id.LastIndexOf("/", StringComparison.OrdinalIgnoreCase) + 1);
                        curId = int.Parse(afterSlash.Substring(0, afterSlash.Length - 2));
                        Assert.True(curId >= proggress.MaxId);
                        usersCount.Add(x);
                        proggress.MaxId = curId;
                    }
                    if (usersCount.Count == 10)
                    {
                        reachedMaxDocCountMre.Set();
                    }
                }
                catch (Exception)
                {
                }
                return Task.CompletedTask;
            };

            var task = subscription.Run(a =>
            {
            });

            //await Task.WhenAny(task, Task.Delay(_reasonableWaitTime)).ConfigureAwait(false);

            return (subscription, task);
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
                var mre = new AsyncManualResetEvent();
                using (var subscriptionManager = new DocumentSubscriptions(store))
                {
                    var reqEx = store.GetRequestExecutor();
                    var name = subscriptionManager.Create(new SubscriptionCreationOptions<User>());

                    var subs = await SubscriptionFailoverWithWaitingChains.GetSubscription(name, store.Database, cluster.Nodes);
                    Assert.NotNull(subs);
                    await WaitForRaftIndexToBeAppliedOnClusterNodes(subs.SubscriptionId, cluster.Nodes);

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

        private async Task WaitForResponsibleNode(ServerStore online, string dbName, string subscriptionName, bool toBecomeNull = false)
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

                    return;
                }
            }
            finally
            {
                sp.Stop();
                Assert.True(sp.ElapsedMilliseconds < _reasonableWaitTime.TotalMilliseconds);
            }
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
    }
}
