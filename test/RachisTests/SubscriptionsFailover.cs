using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using FastTests.Server.Documents.Notifications;
using Raven.Client.Extensions;
using Raven.Client.Server.Operations;
using Raven.Server;
using Raven.Server.Rachis;
using Sparrow;


namespace RachisTests
{
    public class SubscriptionsFailover: ClusterTestBase
    {

        private class SubscriptionProggress
        {
            public int MaxId;
        }
        private readonly TimeSpan _reasonableWaitTime = TimeSpan.FromSeconds(20);

        [Fact]
        public async Task ContinueFromThePointIStopped()
        {
            const int nodesAmount = 5;
            var leader = await this.CreateRaftClusterAndGetLeader(nodesAmount);

            var defaultDatabase = "ContinueFromThePointIStopped";
            
            await CreateDatabaseInCluster(defaultDatabase, nodesAmount, leader.WebUrls[0]).ConfigureAwait(false);
            
            string tag1, tag2, tag3;
            using (var store = new DocumentStore
            {
                Url = leader.WebUrls[0],
                Database = defaultDatabase
            }.Initialize())
            {
                var usersCount = new List<User>();
                var reachedMaxDocCountMre = new AsyncManualResetEvent();
                var subscription = await CreateAndInitiateSubscription(store, defaultDatabase, usersCount, reachedMaxDocCountMre);
                
                await GenerateDocuments(store);

                Assert.True(await reachedMaxDocCountMre.WaitAsync(_reasonableWaitTime));

                tag1 = subscription.CurrentNodeTag;

                usersCount.Clear();
                reachedMaxDocCountMre.Reset();

                await KillServerWhereSubscriptionWorks( defaultDatabase, subscription.SubscriptionId);

                await GenerateDocuments(store);
                Assert.True(await reachedMaxDocCountMre.WaitAsync(_reasonableWaitTime));

                tag2 = subscription.CurrentNodeTag;

                Assert.NotEqual(tag1,tag2);
                usersCount.Clear();
                reachedMaxDocCountMre.Reset();

                await KillServerWhereSubscriptionWorks( defaultDatabase, subscription.SubscriptionId);

                await GenerateDocuments(store);

                Assert.True(await reachedMaxDocCountMre.WaitAsync(_reasonableWaitTime));

                tag3 = subscription.CurrentNodeTag;
                Assert.NotEqual(tag1, tag3);
                Assert.NotEqual(tag2, tag3);

            }
        }

        [Fact]
        public async Task SubscripitonDeletionFromCluster()
        {
            const int nodesAmount = 5;
            var leader = await this.CreateRaftClusterAndGetLeader(nodesAmount);

            var defaultDatabase = "ContinueFromThePointIStopped";

            await CreateDatabaseInCluster(defaultDatabase, nodesAmount, leader.WebUrls[0]).ConfigureAwait(false);
            
            using (var store = new DocumentStore
            {
                Url = leader.WebUrls[0],
                Database = defaultDatabase
            }.Initialize())
            {
                var usersCount = new List<User>();
                var reachedMaxDocCountMre = new AsyncManualResetEvent();

                var subscriptionId = await store.AsyncSubscriptions.CreateAsync(new SubscriptionCreationOptions<User>(){});

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Peter"
                    });
                    await session.SaveChangesAsync();
                }

                var subscription = store.AsyncSubscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId));

                subscription.AfterAcknowledgment += () => reachedMaxDocCountMre.Set();
                subscription.Subscribe(x => { });
                await subscription.StartAsync();

                await reachedMaxDocCountMre.WaitAsync();


                foreach (var ravenServer in Servers)
                {
                    using (ravenServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        Assert.NotNull(ravenServer.ServerStore.Cluster.Read(context, subscriptionId));
                    }
                }

                await subscription.DisposeAsync();

                var deleteResult = store.Admin.Server.Send(new DeleteDatabaseOperation(defaultDatabase, hardDelete: true));

                foreach (var ravenServer in Servers)
                {
                    await ravenServer.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, deleteResult.ETag + nodesAmount).WaitWithTimeout(TimeSpan.FromSeconds(60));
                }
                Thread.Sleep(2000);
                
                foreach (var ravenServer in Servers)
                {
                    using (ravenServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        Assert.Null(ravenServer.ServerStore.Cluster.Read(context, subscriptionId.ToLowerInvariant()));
                    }
                }
            }
        }

        private async Task<Subscription<User>> CreateAndInitiateSubscription(IDocumentStore store, string defaultDatabase, List<User> usersCount, AsyncManualResetEvent reachedMaxDocCountMre)
        {
            var proggress = new SubscriptionProggress()
            {
                MaxId = 0
            };
            var subscriptionId = await store.AsyncSubscriptions.CreateAsync(new SubscriptionCreationOptions<User>()).ConfigureAwait(false);
            var subscriptionEtag = long.Parse(subscriptionId.Substring(subscriptionId.LastIndexOf("/", StringComparison.OrdinalIgnoreCase) + 1));

            var subscription = store.AsyncSubscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId)
            {
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(500)
            });

            var getDatabaseTopologyCommand = new GetDatabaseTopologyOperation(defaultDatabase);
            var topology = await store.Admin.Server.SendAsync(getDatabaseTopologyCommand).ConfigureAwait(false);
            
            foreach (var server in Servers.Where(s => topology.RelevantFor(s.ServerStore.NodeTag)))
            {
                await server.ServerStore.Cluster.WaitForIndexNotification(subscriptionEtag).ConfigureAwait(false);
            }

            subscription.Subscribe(x =>
            {
                int curId = 0;
                curId = int.Parse(x.Id.Substring(x.Id.LastIndexOf("/",StringComparison.OrdinalIgnoreCase) + 1));
                Assert.True(curId > proggress.MaxId);
                usersCount.Add(x);
                proggress.MaxId = curId;
            });
            subscription.AfterAcknowledgment += () =>
            {
                if (usersCount.Count == 10)
                {
                    reachedMaxDocCountMre.Set();
                }
            };
            await subscription.StartAsync().ConfigureAwait(false);
            return subscription;
        }

        private async Task KillServerWhereSubscriptionWorks(string defaultDatabase, string subscriptionId)
        {
            string tag = null;
            var someServer = Servers.First(x => x.Disposed == false);
            using (someServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var databaseRecord = someServer.ServerStore.Cluster.ReadDatabase(context, defaultDatabase);
                var db = await someServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(defaultDatabase).ConfigureAwait(false);
                var subscriptionState = db.SubscriptionStorage.GetSubscriptionFromServerStore(subscriptionId);
                tag = databaseRecord.Topology.WhoseTaskIsIt(subscriptionState);
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
