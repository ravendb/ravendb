using System;
using System.Threading.Tasks;
using FastTests.Client.Subscriptions;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
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

    // a query 'filter' clause is evaluated as JavaScript through the same visitor as a
    // subscription where clause, so it gets the same '@refresh' handling

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData("from Things as t filter t.'@metadata'.'@refresh' = null", DatabaseMode = RavenDatabaseMode.All)]
    [RavenData("from Things filter '@metadata'.'@refresh' = null", DatabaseMode = RavenDatabaseMode.All)]
    [RavenData("from Things as t filter t.'@metadata'.'@refresh' = null and t.Name != 'none'", DatabaseMode = RavenDatabaseMode.All)]
    public async Task CanHandleMetadataRefreshInFilterClause(Options options, string query)
    {
        using (var store = GetDocumentStore(options))
        {
            Assert.Equal(2, await RunFilterQuery(store, query));
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData("from Things as t filter t.'@metadata'.'@refresh' != null", DatabaseMode = RavenDatabaseMode.All)]
    [RavenData("from Things filter '@metadata'.'@refresh' != null", DatabaseMode = RavenDatabaseMode.All)]
    [RavenData("from Things as t filter t.Name != 'none' and not (t.'@metadata'.'@refresh' = null)", DatabaseMode = RavenDatabaseMode.All)]
    public async Task CanHandleNOTMetadataRefreshInFilterClause(Options options, string query)
    {
        using (var store = GetDocumentStore(options))
        {
            Assert.Equal(1, await RunFilterQuery(store, query));
        }
    }

    // the rewrite is not '@refresh' specific - every server owned metadata property is
    // absent rather than null when unset, so all of them need the same treatment.
    // none of the three documents carries '@expires', '@archive-at' or '@archived'.

    [RavenTheory(RavenTestCategory.Subscriptions)]
    [RavenData("'@metadata'.'@expires'", DatabaseMode = RavenDatabaseMode.All)]
    [RavenData("'@metadata'.'@archive-at'", DatabaseMode = RavenDatabaseMode.All)]
    [RavenData("'@metadata'.'@archived'", DatabaseMode = RavenDatabaseMode.All)]
    public async Task CanHandleAnySystemMetadataPropertyInSubscription(Options options, string field)
    {
        using (var store = GetDocumentStore(options))
        {
            var query = $"from Things as t where t.{field} = null";
            Assert.Equal(3, await RunSubscription(store, new SubscriptionCreationOptions { Query = query }));
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData("'@metadata'.'@expires'", DatabaseMode = RavenDatabaseMode.All)]
    [RavenData("'@metadata'.'@archive-at'", DatabaseMode = RavenDatabaseMode.All)]
    [RavenData("'@metadata'.'@archived'", DatabaseMode = RavenDatabaseMode.All)]
    public async Task CanHandleAnySystemMetadataPropertyInFilterClause(Options options, string field)
    {
        using (var store = GetDocumentStore(options))
        {
            await CreateThings(store);

            Assert.Equal(3, await RunQuery(store, $"from Things as t filter t.{field} = null"));
            Assert.Equal(0, await RunQuery(store, $"from Things as t filter t.{field} != null"));
        }
    }

    // a metadata property that is present but explicitly null still counts as null, so the
    // widened comparison has to match it as well as a property that is missing altogether

    [RavenTheory(RavenTestCategory.Subscriptions)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task ExplicitNullMetadataPropertyMatchesInSubscription(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var id = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
            {
                Query = $"from Things as t where t.'@metadata'.'{OriginProperty}' = null"
            });

            await CreateThingsWithOrigin(store);

            await AssertOriginIsPresentAndNull(store);

            // the document with no origin and the one with an explicitly null origin
            Assert.Equal(2, await RunSubscriptionWorker(store, id));
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task ExplicitNullMetadataPropertyMatchesInFilterClause(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            await CreateThingsWithOrigin(store);

            await AssertOriginIsPresentAndNull(store);

            Assert.Equal(2, await RunQuery(store, $"from Things as t filter t.'@metadata'.'{OriginProperty}' = null"));
            Assert.Equal(1, await RunQuery(store, $"from Things as t filter t.'@metadata'.'{OriginProperty}' != null"));
        }
    }

    // intersect(...) combines boolean statements too, so a comparison can sit inside it

    [RavenTheory(RavenTestCategory.Subscriptions)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task CanHandleMetadataRefreshInsideIntersect(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var query = "from Things as t where intersect(t.'@metadata'.'@refresh' = null, t.Name != 'none')";
            Assert.Equal(2, await RunSubscription(store, new SubscriptionCreationOptions { Query = query }));
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task CanHandleMetadataRefreshInsideIntersectInFilterClause(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            await CreateThings(store);

            Assert.Equal(2, await RunQuery(store, "from Things as t filter intersect(t.'@metadata'.'@refresh' = null, t.Name != 'none')"));
            Assert.Equal(1, await RunQuery(store, "from Things as t filter intersect(t.'@metadata'.'@refresh' != null, t.Name != 'none')"));
        }
    }

    // user defined metadata does not get the rewrite, so it compares exactly as written:
    // only the document whose 'Origin' is explicitly null is equal to null, and the one
    // that has no 'Origin' at all reads as undefined and is therefore not equal to null

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task UserDefinedMetadataPropertyIsLeftAlone(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            await CreateThingsWithOrigin(store, "Origin");

            Assert.Equal(1, await RunQuery(store, "from Things as t filter t.'@metadata'.'Origin' = null"));
            Assert.Equal(2, await RunQuery(store, "from Things as t filter t.'@metadata'.'Origin' != null"));
        }
    }

    // guards the explicit null tests: they only prove anything if the server really stored
    // the property as present-but-null rather than dropping it
    private static async Task AssertOriginIsPresentAndNull(IDocumentStore store)
    {
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced
                .AsyncRawQuery<Thing>("from Things as t where t.Name = 'explicit-null'")
                .WaitForNonStaleResults()
                .ToListAsync();

            var metadata = session.Advanced.GetMetadataFor(Assert.Single(results));

            Assert.True(metadata.ContainsKey(OriginProperty), $"'{OriginProperty}' was dropped instead of being stored as null");
            Assert.Null(metadata[OriginProperty]);
        }
    }

    // an '@' prefixed name the server does not manage itself, so it is subject to the
    // rewrite while still being ours to set to whatever the test needs
    private const string OriginProperty = "@origin";

    private static async Task CreateThingsWithOrigin(IDocumentStore store, string property = OriginProperty)
    {
        using (var session = store.OpenAsyncSession())
        {
            var withOrigin = new Thing { Name = "with" };
            await session.StoreAsync(withOrigin);
            session.Advanced.GetMetadataFor(withOrigin)[property] = "web";

            var explicitNull = new Thing { Name = "explicit-null" };
            await session.StoreAsync(explicitNull);
            session.Advanced.GetMetadataFor(explicitNull)[property] = null;

            await session.StoreAsync(new Thing { Name = "missing" });

            await session.SaveChangesAsync();
        }
    }

    private static async Task<int> RunFilterQuery(IDocumentStore store, string query)
    {
        await CreateThings(store);

        return await RunQuery(store, query);
    }

    private static async Task<int> RunQuery(IDocumentStore store, string query)
    {
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced
                .AsyncRawQuery<Thing>(query)
                .WaitForNonStaleResults()
                .ToListAsync();

            return results.Count;
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

        return await RunSubscriptionWorker(store, id);
    }

    private static async Task<int> RunSubscriptionWorker(IDocumentStore store, string id)
    {
        await using (var worker = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
        {
            CloseWhenNoDocsLeft = true
        }))
        {
            var items = 0;
            var run = worker.Run(batch =>
            {
                items += batch.Items.Count;
            });

            var done = await Task.WhenAny(run, Task.Delay(TimeSpan.FromSeconds(30)));
            Assert.Same(run, done);

            // CloseWhenNoDocsLeft closes the worker once it has drained, which surfaces as
            // SubscriptionClosedException. Awaiting it makes any other failure fail the test
            // instead of being swallowed as an unobserved exception.
            await Assert.ThrowsAsync<SubscriptionClosedException>(async () => await run);

            return items;
        }
    }
}
