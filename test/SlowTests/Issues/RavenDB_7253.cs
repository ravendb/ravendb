using System;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client.Attachments;
using FastTests.Server.Documents.Notifications;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Client.Server.Operations;
using Sparrow;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_7253 : RavenTestBase
    {
        [Fact]
        public async Task SubscriptionShouldStopUponDatabaseDeletion()
        {
            using (var store = GetDocumentStore())
            {

                var subscriptionId = await store.AsyncSubscriptions.CreateAsync(new SubscriptionCreationOptions<User>());

                var subscription = store.AsyncSubscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(200)
                });

                var mre = new AsyncManualResetEvent();
                subscription.Subscribe(x => { });

                subscription.AfterAcknowledgment += mre.Set;

                await subscription.StartAsync();

                await mre.WaitAsync(TimeSpan.FromSeconds(20));

                await store.Admin.Server.SendAsync(new DeleteDatabaseOperation(store.Database, hardDelete: true));

                Assert.True(await subscription.SubscriptionLifetimeTask.WaitWithTimeout(TimeSpan.FromSeconds(20)));
            }
        }
    }
}
