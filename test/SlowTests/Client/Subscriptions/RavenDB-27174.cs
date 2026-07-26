using System;
using System.Threading.Tasks;
using FastTests.Client.Subscriptions;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Client.Subscriptions;

public class RavenDB_27174(ITestOutputHelper output) : SubscriptionTestBase(output)
{
    // out of the three documents that are created, exactly one has an '@refresh' metadata entry

    [RavenTheory(RavenTestCategory.Subscriptions)]
    [InlineData("from Things as t where t.'@metadata'.'@refresh' = null")]
    [InlineData("from Things as t where t.'@metadata'.'@refresh' == null")]
    [InlineData("from Things where '@metadata'.'@refresh' = null and Name != 'none'")]
    [InlineData("from Things as t where t.'@metadata'.'@refresh' = null and t.Name != 'none'")]
    public async Task CanHandleMetadataRefreshWithAliasOrCompoundWhere(string query)
    {
        using (var store = GetDocumentStore())
        {
            int items = await RunSubscription(store, new SubscriptionCreationOptions { Query = query });
            Assert.Equal(2, items);
        }
    }

    [RavenTheory(RavenTestCategory.Subscriptions)]
    [InlineData("from Things as t where t.'@metadata'.'@refresh' != null")]
    [InlineData("from Things where '@metadata'.'@refresh' != null and Name != 'none'")]
    [InlineData("from Things as t where t.'@metadata'.'@refresh' != null and t.Name != 'none'")]
    public async Task CanHandleNOTMetadataRefreshWithAliasOrCompoundWhere(string query)
    {
        using (var store = GetDocumentStore())
        {
            int items = await RunSubscription(store, new SubscriptionCreationOptions { Query = query });
            Assert.Equal(1, items);
        }
    }

    [RavenFact(RavenTestCategory.Subscriptions)]
    public async Task CanHandleNegatedMetadataRefresh()
    {
        using (var store = GetDocumentStore())
        {
            // "not (@refresh = null)" is the same as "@refresh != null"
            var query = "from Things as t where t.Name != 'none' and not (t.'@metadata'.'@refresh' = null)";
            int items = await RunSubscription(store, new SubscriptionCreationOptions { Query = query });
            Assert.Equal(1, items);
        }
    }

    private static async Task CreateThings(IDocumentStore store)
    {
        using (var session = store.OpenAsyncSession())
        {
            var future = new Thing { Name = "future" };
            await session.StoreAsync(future);
            session.Advanced.GetMetadataFor(future)["@refresh"] = DateTime.Today.AddDays(5).ToString("O");

            await session.StoreAsync(new Thing { Name = "first" });
            await session.StoreAsync(new Thing { Name = "second" });

            await session.SaveChangesAsync();
        }
    }

    private static async Task<int> RunSubscription(IDocumentStore store, SubscriptionCreationOptions subscriptionCreationParams)
    {
        string id = await store.Subscriptions.CreateAsync(subscriptionCreationParams);

        await CreateThings(store);

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
