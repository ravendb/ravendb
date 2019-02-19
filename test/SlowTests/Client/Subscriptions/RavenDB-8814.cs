using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Xunit;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_8814 : RavenTestBase
    {
        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(10);

        [Fact]
        public async Task ShouldSupportProjectionAndFilter()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<User>()
                {
                    Filter = x => x.Age > 30,
                    Projection = x => new
                    {

                        Name = x.Name,
                        Age = x.Age,
                        Foo = "Bar"
                    }
                });

                string foo = null;

                var subscription = store.Subscriptions.GetSubscriptionWorker<dynamic>(subscriptionName);

                var mre = new AsyncManualResetEvent();
                int resultsCount = 0;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Age = 31,
                        Name = "Vasia"
                    });

                    await session.StoreAsync(new User
                    {
                        Age = 29,
                        Name = "Samson"
                    });
                    await session.SaveChangesAsync();
                }

                _ = subscription.Run(x =>
                {
                    foo = x.Items.First().Result.Foo;
                    resultsCount = x.Items.Count;
                    mre.Set();
                });

                Assert.True(await mre.WaitAsync(_reasonableWaitTime));
                Assert.Equal("Bar", foo);
                Assert.Equal(1, resultsCount);

            }
        }
    }
}
