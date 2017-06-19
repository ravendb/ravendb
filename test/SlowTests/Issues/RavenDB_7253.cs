using System;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client.Attachments;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Client.Server.Operations;
using SlowTests.Server.Documents.Notifications;
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

                subscription.AfterAcknowledgment += mre.Set;

                var task = subscription.Run(user => { });

                await mre.WaitAsync(TimeSpan.FromSeconds(20));

                await store.Admin.Server.SendAsync(new DeleteDatabaseOperation(store.Database, hardDelete: true));

                Assert.True(await task.WaitWithTimeout(TimeSpan.FromSeconds(20)));
            }
        }
    }
}
