using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Revisions;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Rachis;
using Sparrow;
using Sparrow.Json;


namespace RachisTests
{
    public class SubscriptionsFailover : ClusterTestBase
    {

        private class SubscriptionProggress
        {
            public int MaxId;
        }
        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(6);

        [NightlyBuildTheory]
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

            string tag1, tag2, tag3;
            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = defaultDatabase
            }.Initialize())
            {
                var usersCount = new List<User>();
                var reachedMaxDocCountMre = new AsyncManualResetEvent();
                var subscription = await CreateAndInitiateSubscription(store, defaultDatabase, usersCount, reachedMaxDocCountMre, batchSize);

                await GenerateDocuments(store);

                Assert.True(await reachedMaxDocCountMre.WaitAsync(_reasonableWaitTime));

                tag1 = subscription.CurrentNodeTag;

                usersCount.Clear();
                reachedMaxDocCountMre.Reset();

                await KillServerWhereSubscriptionWorks(defaultDatabase, subscription.SubscriptionName.ToString());

                await GenerateDocuments(store);
                Assert.True(await reachedMaxDocCountMre.WaitAsync(_reasonableWaitTime));

                tag2 = subscription.CurrentNodeTag;

                //     Assert.NotEqual(tag1,tag2);
                usersCount.Clear();
                reachedMaxDocCountMre.Reset();

                await KillServerWhereSubscriptionWorks(defaultDatabase, subscription.SubscriptionName);

                await GenerateDocuments(store);

                Assert.True(await reachedMaxDocCountMre.WaitAsync(_reasonableWaitTime));

                tag3 = subscription.CurrentNodeTag;
                // Assert.NotEqual(tag1, tag3);
                //    Assert.NotEqual(tag2, tag3);

            }
        }

        [NightlyBuildFact]
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

                var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId));

                subscription.AfterAcknowledgment += b => { reachedMaxDocCountMre.Set(); return Task.CompletedTask; };

                GC.KeepAlive(subscription.Run(x => { }));

                await reachedMaxDocCountMre.WaitAsync();


                foreach (var ravenServer in Servers)
                {
                    using (ravenServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        Assert.NotNull(ravenServer.ServerStore.Cluster.Read(context, SubscriptionState.GenerateSubscriptionItemKeyName(defaultDatabase, subscriptionId.ToString())));
                    }
                }

                await subscription.DisposeAsync();

                var deleteResult = store.Admin.Server.Send(new DeleteDatabaseOperation(defaultDatabase, hardDelete: true));

                foreach (var ravenServer in Servers)
                {
                    await ravenServer.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, deleteResult.RaftCommandIndex + nodesAmount).WaitWithTimeout(TimeSpan.FromSeconds(60));
                }
                Thread.Sleep(2000);

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
                    GenerateDistributedRevisionsData(defaultDatabase);
                }
            }
        }

        [NightlyBuildTheory]
        [InlineData(3)]
        [InlineData(5)]
        public async Task DistributedRevisionsSubscription(int nodesAmount)
        {
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


                GenerateDistributedRevisionsData(defaultDatabase);



                var subscriptionId = await store.Subscriptions.CreateAsync<Revision<User>>().ConfigureAwait(false);

                var subscription = store.Subscriptions.Open<Revision<User>>(new SubscriptionConnectionOptions(subscriptionId)
                {
                    MaxDocsPerBatch = 1,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(100)
                });

                var docsCount = 0;
                var revisionsCount = 0;
                var expectedRevisionsCount = 0;

                subscription.AfterAcknowledgment += async b =>
                {
                    await continueMre.WaitAsync();

                    try
                    {
                        if (revisionsCount == expectedRevisionsCount)
                        {
                            continueMre.Reset();
                            ackSent.Set();

                        }


                        await continueMre.WaitAsync();
                    }
                    catch (Exception)
                    {

                    }
                };

                var task = subscription.Run(b =>
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

                                docsCount++;
                                revisionsCount++;
                            }
                            else if (x.Current == null)
                            {

                            }
                            else
                            {

                                if (x.Current.Age > x.Previous.Age)
                                {
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
                });

                expectedRevisionsCount = nodesAmount + 2;
                continueMre.Set();
                Assert.True(await task.WaitAsync(_reasonableWaitTime).ConfigureAwait(false));



                Assert.True(await ackSent.WaitAsync(_reasonableWaitTime).ConfigureAwait(false));
                ackSent.Reset(true);

                await KillServerWhereSubscriptionWorks(defaultDatabase, subscription.SubscriptionName).ConfigureAwait(false);
                continueMre.Set();
                expectedRevisionsCount += 2;


                Assert.True(await ackSent.WaitAsync(_reasonableWaitTime).ConfigureAwait(false));
                ackSent.Reset(true);
                continueMre.Set();


                if (nodesAmount == 5)
                    await KillServerWhereSubscriptionWorks(defaultDatabase, subscription.SubscriptionName);

                Assert.True(await reachedMaxDocCountMre.WaitAsync(_reasonableWaitTime).ConfigureAwait(false));

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
                        Active = true,
                        MinimumRevisionsToKeep = 10,
                    },
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        ["Users"] = new RevisionsCollectionConfiguration
                        {
                            Active = true,
                            MinimumRevisionsToKeep = 10
                        }
                    }
                };

                var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(defaultDatabase);
                var res = await documentDatabase.ServerStore.ModifyDatabaseRevisions(context,
                    defaultDatabase,
                    EntityToBlittable.ConvertEntityToBlittable(configuration,
                        new DocumentConventions(),
                        context));

                foreach (var s in Servers)// need to wait for it on all servers
                {
                    documentDatabase = await s.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(defaultDatabase);
                    await documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(res.Item1);
                }
            }
        }

        private void GenerateDistributedRevisionsData(string defaultDatabase)
        {
            IReadOnlyList<ServerNode> nodes;
            using (var store = new DocumentStore
            {
                Urls = new[] { Servers[0].WebUrl },
                Database = defaultDatabase
            })
            {
                AsyncHelpers.RunSync(() => store.GetRequestExecutor()
                    .UpdateTopologyAsync(new ServerNode
                    {
                        Url = store.Urls[0],
                        Database = defaultDatabase,
                    }, Timeout.Infinite));
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

        private async Task<Subscription<User>> CreateAndInitiateSubscription(IDocumentStore store, string defaultDatabase, List<User> usersCount, AsyncManualResetEvent reachedMaxDocCountMre, int batchSize)
        {
            var proggress = new SubscriptionProggress()
            {
                MaxId = 0
            };
            var subscriptionName = await store.Subscriptions.CreateAsync<User>().ConfigureAwait(false);

            var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionName)
            {
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(500),
                MaxDocsPerBatch = batchSize
            });
            var subscripitonState = await store.Subscriptions.GetSubscriptionStateAsync(store.Database, subscriptionName);
            var getDatabaseTopologyCommand = new GetDatabaseRecordOperation(defaultDatabase);
            var record = await store.Admin.Server.SendAsync(getDatabaseTopologyCommand).ConfigureAwait(false);

            foreach (var server in Servers.Where(s => record.Topology.RelevantFor(s.ServerStore.NodeTag)))
            {
                await server.ServerStore.Cluster.WaitForIndexNotification(subscripitonState.SubscriptionId).ConfigureAwait(false);
            }

            var task = subscription.Run(a =>
            {
                foreach (var item in a.Items)
                {
                    var x = item.Result;
                    try
                    {
                        int curId = 0;
                        var afterSlash = x.Id.Substring(x.Id.LastIndexOf("/", StringComparison.OrdinalIgnoreCase) + 1);
                        curId = int.Parse(afterSlash.Substring(0, afterSlash.Length - 2));
                        Assert.True(curId >= proggress.MaxId);// todo: change back to '>'
                        usersCount.Add(x);
                        proggress.MaxId = curId;

                    }
                    catch (Exception)
                    {


                    }
                }
            });
            subscription.AfterAcknowledgment += b =>
           {

               try
               {
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

            await Task.WhenAny(task, Task.Delay(_reasonableWaitTime)).ConfigureAwait(false);

            return subscription;
        }

        private async Task KillServerWhereSubscriptionWorks(string defaultDatabase, string subscriptionName)
        {
            string tag = null;
            var someServer = Servers.First(x => x.Disposed == false);
            using (someServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var databaseRecord = someServer.ServerStore.Cluster.ReadDatabase(context, defaultDatabase);
                var db = await someServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(defaultDatabase).ConfigureAwait(false);
                var subscriptionState = db.SubscriptionStorage.GetSubscriptionFromServerStore(subscriptionName);
                tag = databaseRecord.Topology.WhoseTaskIsIt(subscriptionState, Server.ServerStore.IsPassive());
            }
            Servers.FirstOrDefault(x => x.ServerStore.NodeTag == tag).Dispose();
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
    }
}
