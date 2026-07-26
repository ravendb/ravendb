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
    [RavenData("from Things as t where t.'@metadata'.'@refresh' = null", DatabaseMode = RavenDatabaseMode.All)]
    [RavenData("from Things as t where t.'@metadata'.'@refresh' == null", DatabaseMode = RavenDatabaseMode.All)]
    [RavenData("from Things where '@metadata'.'@refresh' = null and Name != 'none'", DatabaseMode = RavenDatabaseMode.All)]
    [RavenData("from Things as t where t.'@metadata'.'@refresh' = null and t.Name != 'none'", DatabaseMode = RavenDatabaseMode.All)]
    public async Task CanHandleMetadataRefreshWithAliasOrCompoundWhere(Options options, string query)
    {
        using (var store = GetDocumentStore(options))
        {
            int items = await RunSubscription(store, new SubscriptionCreationOptions { Query = query });
            Assert.Equal(2, items);
        }
    }

    [RavenTheory(RavenTestCategory.Subscriptions)]
    [RavenData("from Things as t where t.'@metadata'.'@refresh' != null", DatabaseMode = RavenDatabaseMode.All)]
    [RavenData("from Things where '@metadata'.'@refresh' != null and Name != 'none'", DatabaseMode = RavenDatabaseMode.All)]
    [RavenData("from Things as t where t.'@metadata'.'@refresh' != null and t.Name != 'none'", DatabaseMode = RavenDatabaseMode.All)]
    public async Task CanHandleNOTMetadataRefreshWithAliasOrCompoundWhere(Options options, string query)
    {
        using (var store = GetDocumentStore(options))
        {
            int items = await RunSubscription(store, new SubscriptionCreationOptions { Query = query });
            Assert.Equal(1, items);
        }
    }

    [RavenTheory(RavenTestCategory.Subscriptions)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task CanHandleNegatedMetadataRefresh(Options options)
    {
        using (var store = GetDocumentStore(options))
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
