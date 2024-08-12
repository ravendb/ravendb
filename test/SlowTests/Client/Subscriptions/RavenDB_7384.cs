using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_7384 : RavenTestBase
    {
        public RavenDB_7384(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(6);


        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task DisablingDatabaseShouldCutConnection()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = store.Subscriptions.Create<User>(options: new SubscriptionCreationOptions()
                {
                    Name = "Subs1"
                });

                var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("Subs1")
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });

                var results = new List<User>();
                var mre = new AsyncManualResetEvent();

                using (var session = store.OpenSession())
                {
                    session.Store(new User { });
                    session.SaveChanges();
                }

                var subscriptionTask = subscription.Run(batch =>
                {
                    results.AddRange(batch.Items.Select(i => i.Result).ToArray());
                });

                subscription.AfterAcknowledgment += x =>
                {
                    mre.Set();
                    return Task.CompletedTask;
                };

                Assert.True(await mre.WaitAsync(_reasonableWaitTime));

                var currentDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                var subscriptionState = currentDatabase.SubscriptionStorage.GetSubscriptionFromServerStore(subscriptionName);
                var operationIndex = await currentDatabase.SubscriptionStorage.PutSubscription(new SubscriptionCreationOptions()
                {
                    Name = "Subs1",
                    ChangeVector = Raven.Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange.ToString(),
                    Query = "from Users"
                }, Guid.NewGuid().ToString(), subscriptionState.SubscriptionId, true);

                Assert.Equal(subscriptionTask, await Task.WhenAny(subscriptionTask, Task.Delay(_reasonableWaitTime)));

                await Assert.ThrowsAsync(typeof(SubscriptionClosedException), () => subscriptionTask);
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task UpdatingSubscriptionScriptShouldNotChangeVector()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = store.Subscriptions.Create<User>(options: new SubscriptionCreationOptions()
                {
                    Name = "Subs1",
                    Query = "from Users as u select {Name:'David'}"
                });

                var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("Subs1")
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });

                var results = new List<User>();
                var mre = new AsyncManualResetEvent();

                using (var session = store.OpenSession())
                {
                    session.Store(new User { });
                    session.SaveChanges();
                }

                var subscriptionTask = subscription.Run(batch =>
                {
                    results.AddRange(batch.Items.Select(i => i.Result).ToArray());
                });

                subscription.AfterAcknowledgment += x =>
                {
                    mre.Set();
                    return Task.CompletedTask;
                };

                Assert.True(await mre.WaitAsync(_reasonableWaitTime));
                mre.Reset();
                Assert.Equal("David", results[0].Name);
                results.Clear();
                var currentDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                string changeVectorBeforeScriptUpdate = GetSubscriptionChangeVector(currentDatabase);

                var subscriptionState = currentDatabase.SubscriptionStorage.GetSubscriptionFromServerStore(subscriptionName);
                subscription.OnSubscriptionConnectionRetry += x =>
                {
                    var sce = x as SubscriptionClosedException;
                    Assert.NotNull(sce);
                    Assert.Equal(typeof(SubscriptionClosedException), x.GetType());
                    Assert.True(sce.CanReconnect);
                    Assert.Equal($"Subscription With Id '{subscriptionState.SubscriptionName}' was closed.  Raven.Client.Exceptions.Documents.Subscriptions.SubscriptionClosedException: The subscription {subscriptionState.SubscriptionName} query has been modified, connection must be restarted", x.Message);
                };

                const string newQuery = "from Users as u select {Name:'Jorgen'}";

                // updating only subscription script and making sure connection drops
                await currentDatabase.SubscriptionStorage.PutSubscription(new SubscriptionCreationOptions()
                {
                    Name = "Subs1",
                    ChangeVector = Raven.Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange.ToString(),
                    Query = newQuery
                }, Guid.NewGuid().ToString(), subscriptionState.SubscriptionId);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store, store.Database);
                using (db.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var query = WaitForValue(() =>
                    {
                        var connectionState = db.SubscriptionStorage.GetSubscriptionConnectionsState(ctx, subscriptionState.SubscriptionName);

                        return connectionState?.GetConnections().First()?.SubscriptionState.Query;
                    }, newQuery);

                    Assert.Equal(newQuery, query);
                }
                var newSubscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                var newState = newSubscriptions.First();
                Assert.Equal(1, newSubscriptions.Count);
                Assert.Equal(subscriptionState.SubscriptionName, newState.SubscriptionName);
                Assert.Equal(newQuery, newState.Query);
                Assert.Equal(subscriptionState.SubscriptionId, newState.SubscriptionId);
                var changeVectorAfterUpdatingScript = GetSubscriptionChangeVector(currentDatabase);
                Assert.Equal(changeVectorBeforeScriptUpdate, changeVectorAfterUpdatingScript);

                subscription.AfterAcknowledgment += x =>
                {
                    mre.Set();
                    return Task.CompletedTask;
                };


                using (var session = store.OpenSession())
                {
                    session.Store(new User { });
                    session.SaveChanges();
                }

                Assert.True(await mre.WaitAsync(_reasonableWaitTime));
                Assert.Equal("Jorgen", results[0].Name);
            }
        }

        private string GetSubscriptionChangeVector(DocumentDatabase currentDatabase)
        {
            using (Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                var subscriptionData = currentDatabase.SubscriptionStorage.GetSubscriptionByName(context, "Subs1");
                return subscriptionData.ChangeVectorForNextBatchStartingPoint;
            }
        }
    }
}

