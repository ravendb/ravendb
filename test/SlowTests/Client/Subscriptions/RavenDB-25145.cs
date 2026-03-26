using System;
using System.Threading.Tasks;
using FastTests.Client.Subscriptions;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Client.Subscriptions;

public class RavenDB_25145(ITestOutputHelper output) : SubscriptionTestBase(output)
{
    [RavenFact(RavenTestCategory.Subscriptions)]
    public async Task CanHandleMetadataRefresh()
    {
        using (var store = GetDocumentStore())
        {
            var subscriptionCreationParams = new SubscriptionCreationOptions
            {
                Query = "from Things where '@metadata'.'@refresh' = null"
            };
            int items = await RunSubscription(store, subscriptionCreationParams);
            Assert.Equal(2, items);
        }
    }
    
    [RavenFact(RavenTestCategory.Subscriptions)]
    public async Task CanHandleNOTMetadataRefresh()
    {
        using (var store = GetDocumentStore())
        {
            var subscriptionCreationParams = new SubscriptionCreationOptions
            {
                Query = "from Things where '@metadata'.'@refresh' != null"
            };
            int items = await RunSubscription(store, subscriptionCreationParams);
            Assert.Equal(1, items);
        }
    }

    private static async Task<int> RunSubscription(DocumentStore store, SubscriptionCreationOptions subscriptionCreationParams)
    {
        string id = await store.Subscriptions.CreateAsync(subscriptionCreationParams);
        using (var s = store.OpenAsyncSession())
        {
            Thing future = new Thing();
            await s.StoreAsync(future);
            s.Advanced.GetMetadataFor(future)["@refresh"] = DateTime.Today.AddDays(5).ToString("O");
            await s.StoreAsync(new Thing());
            await s.StoreAsync(new Thing());
            await s.SaveChangesAsync();
        }

        var worker = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
        {
            CloseWhenNoDocsLeft = true
        });
        var items = 0;
        var t = worker.Run(batch =>
        {
            items += batch.Items.Count;
        });
        var done = await Task.WhenAny(t, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.Same(t, done);
        return items;
    }
}
