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
using Xunit;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_7384 : RavenTestBase
    {
        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(6);


        [Fact]
        public async Task DisablingDatabaseShouldCutConnection()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = store.Subscriptions.Create<User>(options: new SubscriptionCreationOptions()
                {
                    Name = "Subs1"
                });

                var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("Subs1") {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });

                var results = new List<User>();
                var mre = new AsyncManualResetEvent();

                using (var session = store.OpenSession())
                {
                    session.Store(new User{});
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
                }, subscriptionState.SubscriptionId, true);

                Assert.Equal(subscriptionTask, await Task.WhenAny(subscriptionTask, Task.Delay(_reasonableWaitTime)));

                await Assert.ThrowsAsync(typeof(SubscriptionClosedException), () => subscriptionTask);
            }
        }

        [Fact]
        public async Task UpdatingSubscriptionScriptShouldNotChangeVectorButShouldDropConnection()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = store.Subscriptions.Create<User>(options: new SubscriptionCreationOptions()
                {
                    Name = "Subs1",
                    Query = "from Users as u select {Name:'David'}"
                });

                var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("Subs1") {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });

                var results = new List<User>();
                var mre = new AsyncManualResetEvent();

                using (var session = store.OpenSession())
                {
                    session.Store(new User{});
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
                Assert.Equal("David",results[0].Name);
                results.Clear();
                var currentDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                string changeVectorBeforeScriptUpdate = GetSubscriptionChangeVector(currentDatabase);

                var subscriptionState = currentDatabase.SubscriptionStorage.GetSubscriptionFromServerStore(subscriptionName);


                // updating only subscription script and making sure connection drops
                await currentDatabase.SubscriptionStorage.PutSubscription(new SubscriptionCreationOptions()
                {
                    Name = "Subs1",
                    ChangeVector = Raven.Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange.ToString(),
                    Query = "from Users as u select {Name:'Jorgen'}"

                }, subscriptionState.SubscriptionId);

                Assert.Equal(subscriptionTask, await Task.WhenAny(subscriptionTask, Task.Delay(_reasonableWaitTime)));

                await Assert.ThrowsAsync(typeof(SubscriptionClosedException), () => subscriptionTask);

                var changeVectorAfterUpdatingScript = GetSubscriptionChangeVector(currentDatabase);
                Assert.Equal(changeVectorBeforeScriptUpdate, changeVectorAfterUpdatingScript);


                // reconnecting and making sure that the new script is in power
                subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("Subs1") {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });

                subscriptionTask = subscription.Run(batch =>
                {
                    results.AddRange(batch.Items.Select(i => i.Result).ToArray());
                });

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
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var subscriptionData = currentDatabase.SubscriptionStorage.GetSubscriptionFromServerStore(context, "Subs1");
                return subscriptionData.ChangeVectorForNextBatchStartingPoint;
            }
        }
    }
}

