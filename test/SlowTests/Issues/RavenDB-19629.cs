using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19629 : RavenTestBase
{
    public RavenDB_19629(ITestOutputHelper output) : base(output)
    {
    }
    
    [Fact]
    public async Task TestSessionMixture2()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession(new SessionOptions
                   {
                       TransactionMode = TransactionMode.ClusterWide
                   }))
            {
                await session.StoreAsync(new User(),"foo/bar");
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
                await session.StoreAsync(new User(),"foo/bar");
                await session.SaveChangesAsync();
            }
        }
    }


    private record User();
}
