using System;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Sparrow.Server;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13478 : RavenTestBase
    {
        public RavenDB_13478(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanIncludeCountersInSubscriptions()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var product = new Product();
                    session.Store(product);

                    session.CountersFor(product).Increment("Likes", 3);
                    session.CountersFor(product).Increment("Dislikes", 5);

                    session.SaveChanges();
                }

                var name = store.Subscriptions.Create(new SubscriptionCreationOptions<Product>
                {
                    Includes = builder => builder
                        .IncludeCounter("Likes")
                        .IncludeCounter("Dislikes")
                });

                await AssertSubscription(store, name, 0);

                name = store.Subscriptions.Create(new SubscriptionCreationOptions<Product>
                {
                    Includes = builder => builder
                        .IncludeAllCounters()
                });

                await AssertSubscription(store, name, 0);

                name = store.Subscriptions.Create(new SubscriptionCreationOptions<Product>
                {
                    Includes = builder => builder
                        .IncludeCounter("Likes")
                });

                await AssertSubscription(store, name, 1);

                name = store.Subscriptions.Create(new SubscriptionCreationOptions<Product>());

                await AssertSubscription(store, name, 2);
            }

            static async Task AssertSubscription(IDocumentStore store, string name, int expectedNumberOfRequests)
            {
                using (var sub = store.Subscriptions.GetSubscriptionWorker<Product>(name))
                {
                    var mre = new AsyncManualResetEvent();
                    var r = sub.Run(batch =>
                    {
                        Assert.NotEmpty(batch.Items);
                        using (var s = batch.OpenSession())
                        {
                            foreach (var item in batch.Items)
                            {
                                var product = s.Load<Product>(item.Id);
                                Assert.Same(product, item.Result);

                                var likesValue = s.CountersFor(product).Get("Likes");
                                Assert.Equal(3, likesValue);

                                var dislikesValue = s.CountersFor(product).Get("Dislikes");
                                Assert.Equal(5, dislikesValue);
                            }
                            Assert.Equal(expectedNumberOfRequests, s.Advanced.NumberOfRequests);
                        }
                        mre.Set();
                    });
                    Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(30)));
                    await sub.DisposeAsync();
                    await r;// no error
                }
            }
        }
    }
}
