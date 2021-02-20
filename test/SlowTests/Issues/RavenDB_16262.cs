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
    public class RavenDB_16262 : RavenTestBase
    {
        public RavenDB_16262(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanIncludeCountersInSubscriptions_EvenIfTheyDoNotExist()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var product = new Product();
                    session.Store(product, "products/1");

                    session.SaveChanges();
                }

                var name = store.Subscriptions.Create(new SubscriptionCreationOptions<Product>
                {
                    Includes = builder => builder
                        .IncludeCounter("Likes")
                        .IncludeCounter("Dislikes")
                });

                await AssertSubscription(store, name, 0);
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
                                Assert.Null(likesValue);

                                var dislikesValue = s.CountersFor(product).Get("Dislikes");
                                Assert.Null(dislikesValue);
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
