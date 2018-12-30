using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client;
using Raven.Client.Documents.Subscriptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12259 : RavenTestBase
    {
        [Fact]
        public async Task ProjectionsShouldHaveProperMetadataSetInSubscriptions()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company
                    {
                        Name = "HR"
                    }, "companies/1");

                    await session.SaveChangesAsync();
                }

                var name = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<Company>
                {
                    Projection = company => new { Name = company.Name }
                });

                var mre = new ManualResetEventSlim();

                using (var worker = store.Subscriptions.GetSubscriptionWorker(name))
                {
                    var r = worker.Run(batch =>
                    {
                        Assert.Equal(1, batch.NumberOfItemsInBatch);
                        Assert.Equal(1, batch.Items.Count);

                        var item = batch.Items[0];

                        Assert.Equal("HR", item.RawResult["Name"].ToString());
                        Assert.True(item.Metadata.GetBoolean(Constants.Documents.Metadata.Projection));

                        using (var s = batch.OpenSession())
                        {
                            Assert.Equal(0, s.Advanced.NumberOfRequests);
                            var company = s.Load<Company>("companies/1");
                            Assert.Equal(1, s.Advanced.NumberOfRequests);
                        }

                        mre.Set();
                    });

                    Assert.True(mre.Wait(TimeSpan.FromSeconds(10)));
                    await worker.DisposeAsync();
                    await r;// no error
                }

                mre.Reset();

                name = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<Company>());

                using (var worker = store.Subscriptions.GetSubscriptionWorker(name))
                {
                    var r = worker.Run(batch =>
                    {
                        Assert.Equal(1, batch.NumberOfItemsInBatch);
                        Assert.Equal(1, batch.Items.Count);

                        var item = batch.Items[0];

                        Assert.Equal("HR", item.RawResult["Name"].ToString());
                        Assert.False(item.Metadata.ContainsKey(Constants.Documents.Metadata.Projection));

                        using (var s = batch.OpenSession())
                        {
                            Assert.Equal(0, s.Advanced.NumberOfRequests);
                            var company = s.Load<Company>("companies/1");
                            Assert.Equal(0, s.Advanced.NumberOfRequests);
                        }

                        mre.Set();
                    });

                    Assert.True(mre.Wait(TimeSpan.FromSeconds(10)));
                    await worker.DisposeAsync();
                    await r;// no error
                }
            }
        }
    }
}
