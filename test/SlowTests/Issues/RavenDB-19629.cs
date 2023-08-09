using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19629 : RavenTestBase
{
    public RavenDB_19629(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.ClusterTransactions | RavenTestCategory.ClientApi)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task CanDeleteCmpXchgValue2(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
        {
            await session.StoreAsync(new User { Name = "arava" }, "users/arava");
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            session.Delete("users/arava");
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "arava-new" }, "users/arava");
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
        {
            var arava = await session.LoadAsync<User>("users/arava");
            arava.Name = "new-new";
            await session.SaveChangesAsync();
        }
    }

    [RavenTheory(RavenTestCategory.ClusterTransactions | RavenTestCategory.ClientApi)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task TestSessionMixture2(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                await session.StoreAsync(new User(), "foo/bar");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                session.Delete("foo/bar");
                await session.SaveChangesAsync();
            }
            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                var user = await session.LoadAsync<User>("foo/bar");
                Assert.Null(user);
                await session.StoreAsync(new User(), "foo/bar");
                await session.SaveChangesAsync();
            }
        }
    }

    private class User
    {
        public string Name;
    };
}
