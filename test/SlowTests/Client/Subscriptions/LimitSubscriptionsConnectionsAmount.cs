using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Client.Subscriptions;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Xunit;

namespace SlowTests.Client.Subscriptions
{
    public class LimitSubscriptionsConnectionsAmount : SubscriptionTestBase
    {
        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(6);
        [Fact]
        public async Task Run()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(c => c.Subscriptions.MaxNumberOfConcurrentConnections)] = "4"
            }))
            {
                var mres = new List<AsyncManualResetEvent>();
                var subscriptionTasks = new List<(Task RunTask, SubscriptionWorker<User> SubscriptionObject)>();

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }
                for (var i = 0; i < 4; i++)
                {
                    var curMre = new AsyncManualResetEvent();
                    mres.Add(curMre);
                    subscriptionTasks.Add(OpenAndRunSubscription(store, curMre.Set));
                }

                for (var i = 0; i < 4; i++)
                {
                    var curMre = mres[i];
                    Assert.True(await curMre.WaitAsync(_reasonableWaitTime));
                }

                var subscriptionTask = OpenAndRunSubscription(store, () => { }).RunTask;
                Assert.Equal(subscriptionTask, await Task.WhenAny(subscriptionTask, Task.Delay(_reasonableWaitTime)));
                await Assert.ThrowsAsync(typeof(SubscriptionClosedException), () => subscriptionTask);


                subscriptionTasks[0].SubscriptionObject.Dispose();

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var sp = Stopwatch.StartNew();
                    var subscriptionsCount = 0;

                    while (sp.Elapsed < _reasonableWaitTime && subscriptionsCount != 3)
                    {
                        subscriptionsCount = (await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database)).SubscriptionStorage
                            .GetAllRunningSubscriptions(context, false, 0, 1024).Count();
                    }


                    Assert.Equal(3, subscriptionsCount);
                }
                var nowItConnectsMre = new AsyncManualResetEvent();

                OpenAndRunSubscription(store, nowItConnectsMre.Set);

                Assert.True(await nowItConnectsMre.WaitAsync(_reasonableWaitTime));

            }
        }

        private (Task RunTask, SubscriptionWorker<User> SubscriptionObject) OpenAndRunSubscription(IDocumentStore store, Action runAction)
        {
            var subscriptionId = store.Subscriptions.Create<User>();

            var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subscriptionId)
            {
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(500)
            });

            subscription.AfterAcknowledgment += batch =>
            {
                runAction();
                return Task.CompletedTask;
            };


            return (subscription.Run(x =>
            {
                //noop
            }), subscription);

        }
    }
}
