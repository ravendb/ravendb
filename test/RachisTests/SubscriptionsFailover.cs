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


namespace RachisTests
{
    public class SubscriptionsFailover: ClusterTestBase
    {
        private readonly TimeSpan _reasonableWaitTime = TimeSpan.FromSeconds(60);
        [Fact]
        public async Task ContinueFromThePointIStopped()
        {
            var leader = await this.CreateRaftClusterAndGetLeader(5);

            var defaultDatabase = "ContinueFromThePointIStopped";
            var databaseCreationResult = await CreateDatabaseInCluster(defaultDatabase, 5, leader.WebUrls[0]);
            
            using (var store = new DocumentStore
            {
                Url = leader.WebUrls[0],
                DefaultDatabase = defaultDatabase
            }.Initialize())
            {
                var subscriptionId = await store.AsyncSubscriptions.CreateAsync(new SubscriptionCreationParams<User>());
                var subscriptionEtag = long.Parse(subscriptionId.Substring(subscriptionId.LastIndexOf("/") + 1));

                var subscription = store.AsyncSubscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId));
                
                foreach (var server in Servers.Where(s =>
                    store.GetRequestExecuter(defaultDatabase).TopologyNodes.Any(x => x.ClusterTag == s.ServerStore.NodeTag)))
                
                {
                    await server.ServerStore.Cluster.WaitForIndexNotification(subscriptionEtag);
                }

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        await session.StoreAsync(new User()
                        {
                            Name = "John"+i
                        });
                    }
                    await session.SaveChangesAsync();
                }
                var usersCount = new List<User>();
                var mre = new ManualResetEvent(false);
                subscription.Subscribe(x =>
                {
                    usersCount.Add(x);
                });
                subscription.AfterAcknowledgment += () =>
                {
                    if (usersCount.Count == 10)
                    {
                        mre.Set();
                    }
                };
                await subscription.StartAsync();
                mre.WaitOne(_reasonableWaitTime);
                usersCount.Clear();
                mre.Reset();

                using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var databaseRecord = leader.ServerStore.Cluster.ReadDatabase(context, defaultDatabase);
                    var db = await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(defaultDatabase);
                    var subscriptionState = db.SubscriptionStorage.GetSubscriptionFromServerStore(subscriptionId);
                    var tag = databaseRecord.Topology.WhoseTaskIsIt(subscriptionState);

                    Servers.FirstOrDefault(x => x.ServerStore.NodeTag == tag).Dispose();
                }

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        await session.StoreAsync(new User()
                        {
                            Name = "John" + i
                        });
                    }
                    await session.SaveChangesAsync();
                }

                mre.WaitOne(_reasonableWaitTime);
                
            }
        }
    }
}
