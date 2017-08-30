using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
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
                var subscriptionId = await store.Subscriptions.CreateAsync<User>();
                var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(200)
                });

                var mre = new AsyncManualResetEvent();
                subscription.AfterAcknowledgment += b => { mre.Set(); return Task.CompletedTask; };
                var task = subscription.Run(user => { });

                await mre.WaitAsync(TimeSpan.FromSeconds(20));

                await store.Admin.Server.SendAsync(new DeleteDatabaseOperation(store.Database, hardDelete: true));

                Assert.True(await task.WaitWithTimeout(TimeSpan.FromSeconds(20)));
            }
        }
    }
}
