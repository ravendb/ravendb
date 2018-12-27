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
                    });

                    await session.SaveChangesAsync();
                }

                var name = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<Company>
                {
                    Projection = company => new { Name = company.Name }
                });

                var mre = new ManualResetEventSlim();

                using (var worker = store.Subscriptions.GetSubscriptionWorker(name))
                {
#pragma warning disable 4014
                    worker.Run(batch =>
#pragma warning restore 4014
                    {
                        Assert.Equal(1, batch.NumberOfItemsInBatch);
                        Assert.Equal(1, batch.Items.Count);

                        var item = batch.Items[0];

                        Assert.Equal("HR", item.RawResult["Name"].ToString());
                        Assert.True(item.Metadata.GetBoolean(Constants.Documents.Metadata.Projection));

                        mre.Set();
                    });

                    Assert.True(mre.Wait(TimeSpan.FromSeconds(10)));
                }

                mre.Reset();

                name = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<Company>());

                using (var worker = store.Subscriptions.GetSubscriptionWorker(name))
                {
#pragma warning disable 4014
                    worker.Run(batch =>
#pragma warning restore 4014
                    {
                        Assert.Equal(1, batch.NumberOfItemsInBatch);
                        Assert.Equal(1, batch.Items.Count);

                        var item = batch.Items[0];

                        Assert.Equal("HR", item.RawResult["Name"].ToString());
                        Assert.False(item.Metadata.ContainsKey(Constants.Documents.Metadata.Projection));

                        mre.Set();
                    });

                    Assert.True(mre.Wait(TimeSpan.FromSeconds(10)));
                }
            }
        }
    }
}
